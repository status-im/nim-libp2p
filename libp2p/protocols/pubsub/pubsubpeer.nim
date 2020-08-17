## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[hashes, options, strutils, tables]
import chronos, chronicles, nimcrypto/sha2, metrics
import rpc/[messages, message, protobuf],
       timedcache,
       ../../switch,
       ../../peerid,
       ../../peerinfo,
       ../../stream/connection,
       ../../crypto/crypto,
       ../../protobuf/minprotobuf,
       ../../utility

logScope:
  topics = "pubsubpeer"

when defined(libp2p_expensive_metrics):
  declareCounter(libp2p_pubsub_sent_messages, "number of messages sent", labels = ["id", "topic"])
  declareCounter(libp2p_pubsub_received_messages, "number of messages received", labels = ["id", "topic"])
  declareCounter(libp2p_pubsub_skipped_received_messages, "number of received skipped messages", labels = ["id"])
  declareCounter(libp2p_pubsub_skipped_sent_messages, "number of sent skipped messages", labels = ["id"])

const
  DefaultSendTimeout* = 10.seconds

type
  PubSubObserver* = ref object
    onRecv*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}
    onSend*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}

  PubSubPeer* = ref object of RootObj
    switch*: Switch                     # switch instance to dial peers
    codec*: string                      # the protocol that this peer joined from
    sendConn: Connection                # cached send connection
    peerId*: PeerID
    handler*: RPCHandler
    sentRpcCache: TimedCache[string]    # cache for already sent messages
    recvdRpcCache: TimedCache[string]   # cache for already received messages
    observers*: ref seq[PubSubObserver] # ref as in smart_ptr
    subscribed*: bool                   # are we subscribed to this peer

  RPCHandler* = proc(peer: PubSubPeer, msg: seq[RPCMsg]): Future[void] {.gcsafe.}

func hash*(p: PubSubPeer): Hash =
  # int is either 32/64, so intptr basically, pubsubpeer is a ref
  cast[pointer](p).hash

proc id*(p: PubSubPeer): string =
  doAssert(not p.isNil, "nil pubsubpeer")
  p.peerId.pretty

proc connected*(p: PubSubPeer): bool =
  not p.sendConn.isNil and not
    (p.sendConn.closed or p.sendConn.atEof)

proc recvObservers(p: PubSubPeer, msg: var RPCMsg) =
  # trigger hooks
  if not(isNil(p.observers)) and p.observers[].len > 0:
    for obs in p.observers[]:
      if not(isNil(obs)): # TODO: should never be nil, but...
        obs.onRecv(p, msg)

proc sendObservers(p: PubSubPeer, msg: var RPCMsg) =
  # trigger hooks
  if not(isNil(p.observers)) and p.observers[].len > 0:
    for obs in p.observers[]:
      if not(isNil(obs)): # TODO: should never be nil, but...
        obs.onSend(p, msg)

proc handle*(p: PubSubPeer, conn: Connection) {.async.} =
  logScope:
    peer = p.id

  debug "starting pubsub read loop for peer", closed = conn.closed
  try:
    try:
      while not conn.atEof:
        trace "waiting for data", closed = conn.closed
        let data = await conn.readLp(64 * 1024)
        let digest = $(sha256.digest(data))
        trace "read data from peer", data = data.shortLog
        if digest in p.recvdRpcCache:
          when defined(libp2p_expensive_metrics):
            libp2p_pubsub_skipped_received_messages.inc(labelValues = [p.id])
          trace "message already received, skipping"
          continue

        var rmsg = decodeRpcMsg(data)
        if rmsg.isErr():
          notice "failed to decode msg from peer"
          break

        var msg = rmsg.get()

        trace "decoded msg from peer", msg = msg.shortLog
        # trigger hooks
        p.recvObservers(msg)

        when defined(libp2p_expensive_metrics):
          for m in msg.messages:
            for t in m.topicIDs:
              # metrics
              libp2p_pubsub_received_messages.inc(labelValues = [p.id, t])

        await p.handler(p, @[msg])
        p.recvdRpcCache.put(digest)
    finally:
      debug "exiting pubsub peer read loop"
      await conn.close()

      if p.sendConn == conn:
        p.sendConn = nil

  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "Exception occurred in PubSubPeer.handle", exc = exc.msg

proc getSendConn(p: PubSubPeer): Future[Connection] {.async.} =
  # get a cached send connection or create a new one
  block: # check if there's an existing connection that can be reused
    let current = p.sendConn

    if not current.isNil:
      if not (current.closed() or current.atEof):
        # The existing send connection looks like it might work - reuse it
        return current

      # Send connection is set but broken - get rid of it
      p.sendConn = nil

      # Careful, p.sendConn might change after here!
      await current.close() # TODO this might be unnecessary

  # Grab a new send connection
  let newConn = await p.switch.dial(p.peerId, p.codec) # ...and here
  if newConn == nil:
    return p.sendConn # A concurrent attempt perhaps succeeded?

  # Because of the awaits above, a concurrent `getSendConn` call might have
  # set up a send connection already. We cannot take a lock here because
  # it might block the reading of data from mplex which will cause its
  # backpressure handling to stop reading from the socket and thus prevent the
  # channel negotiation from finishing
  if p.sendConn != nil and not(p.sendConn.closed or p.sendConn.atEof):
    let current = p.sendConn
    # Either the new or the old connection could potentially be closed - it's
    # slightly easier to sequence the closing of the new connection because the
    # old one might still be in use.
    await newConn.close()
    return current

  p.sendConn = newConn
  asyncCheck p.handle(newConn) # start a read loop on the new connection

  return newConn

proc send*(
  p: PubSubPeer,
  msg: RPCMsg,
  timeout: Duration = DefaultSendTimeout) {.async.} =

  doAssert(not isNil(p), "pubsubpeer nil!")

  logScope:
    peer = p.id
    rpcMsg = shortLog(msg)

  trace "sending msg to peer"

  # trigger send hooks
  var mm = msg # hooks can modify the message
  p.sendObservers(mm)

  let encoded = encodeRpcMsg(mm)
  if encoded.len <= 0:
    info "empty message, skipping"
    return

  logScope:
    encoded = shortLog(encoded)

  let digest = $(sha256.digest(encoded))
  if digest in p.sentRpcCache:
    trace "message already sent to peer, skipping"
    when defined(libp2p_expensive_metrics):
      libp2p_pubsub_skipped_sent_messages.inc(labelValues = [p.id])
    return

  var conn: Connection
  try:
    trace "about to send message"
    conn = await p.getSendConn()

    if conn == nil:
      debug "Couldn't get send connection, dropping message"
      return
    trace "sending encoded msgs to peer"
    await conn.writeLp(encoded).wait(timeout)

    p.sentRpcCache.put(digest)
    trace "sent pubsub message to remote"

    when defined(libp2p_expensive_metrics):
      for x in mm.messages:
        for t in x.topicIDs:
          # metrics
          libp2p_pubsub_sent_messages.inc(labelValues = [p.id, t])

  except CatchableError as exc:
    trace "unable to send to remote", exc = exc.msg
    # Next time sendConn is used, it will be have its close flag set and thus
    # will be recycled
    if not isNil(conn):
      await conn.close()

    raise exc

proc `$`*(p: PubSubPeer): string =
  p.id

proc newPubSubPeer*(peerId: PeerID,
                    switch: Switch,
                    codec: string): PubSubPeer =
  new result
  result.switch = switch
  result.codec = codec
  result.peerId = peerId
  result.sentRpcCache = newTimedCache[string](2.minutes)
  result.recvdRpcCache = newTimedCache[string](2.minutes)
