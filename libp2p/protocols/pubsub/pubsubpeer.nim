## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[hashes, options, sequtils, strutils, tables]
import chronos, chronicles, nimcrypto/sha2, metrics, protobuf_serialization
import rpc/[messages, message],
       timedcache,
       ../../peerid,
       ../../peerinfo,
       ../../stream/connection,
       ../../crypto/crypto,
       ../../utility

logScope:
  topics = "pubsubpeer"

declareCounter(libp2p_pubsub_sent_messages, "number of messages sent", labels = ["id", "topic"])
declareCounter(libp2p_pubsub_received_messages, "number of messages received", labels = ["id", "topic"])
declareCounter(libp2p_pubsub_skipped_received_messages, "number of received skipped messages", labels = ["id"])
declareCounter(libp2p_pubsub_skipped_sent_messages, "number of sent skipped messages", labels = ["id"])

type
  PubSubObserver* = ref object
    onRecv*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}
    onSend*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}

  PubSubPeer* = ref object of RootObj
    proto*: string                      # the protocol that this peer joined from
    sendConn: Connection
    peerInfo*: PeerInfo
    handler*: RPCHandler
    sentRpcCache: TimedCache[string]    # cache for already sent messages
    recvdRpcCache: TimedCache[string]   # cache for already received messages
    onConnect*: AsyncEvent
    observers*: ref seq[PubSubObserver] # ref as in smart_ptr

  RPCHandler* = proc(peer: PubSubPeer, msg: seq[RPCMsg]): Future[void] {.gcsafe.}

func hash*(p: PubSubPeer): Hash =
  # int is either 32/64, so intptr basically, pubsubpeer is a ref
  cast[pointer](p).hash

proc id*(p: PubSubPeer): string = p.peerInfo.id

proc connected*(p: PubSubPeer): bool =
  not(isNil(p.sendConn))

proc `conn=`*(p: PubSubPeer, conn: Connection) =
  if not(isNil(conn)):
    trace "attaching send connection for peer", peer = p.id
    p.sendConn = conn
    p.onConnect.fire()

proc conn*(p: PubSubPeer): Connection =
  p.sendConn

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
      while not conn.closed:
        trace "waiting for data", closed = conn.closed
        let data = await conn.readLp(64 * 1024)
        let digest = $(sha256.digest(data))
        trace "read data from peer", data = data.shortLog
        if digest in p.recvdRpcCache:
          libp2p_pubsub_skipped_received_messages.inc(labelValues = [p.id])
          trace "message already received, skipping"
          continue

        var msg: RPCMsg
        try:
          msg = Protobuf.decode(data, RPCMsg)
        except ProtobufReadError:
          notice "failed to decode msg from peer"
          break

        trace "decoded msg from peer", msg = msg.shortLog
        # trigger hooks
        p.recvObservers(msg)

        for m in msg.messages:
          for t in m.topicIDs:
            # metrics
            libp2p_pubsub_received_messages.inc(labelValues = [p.id, t])

        await p.handler(p, @[msg])
        p.recvdRpcCache.put(digest)
    finally:
      debug "exiting pubsub peer read loop"
      await conn.close()

  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "Exception occurred in PubSubPeer.handle", exc = exc.msg
    raise exc

proc send*(p: PubSubPeer, msg: RPCMsg) {.async.} =
  logScope:
    peer = p.id
    msg = shortLog(msg)

  trace "sending msg to peer"

  # trigger send hooks
  var mm = msg # hooks can modify the message
  p.sendObservers(mm)

  let encoded = Protobuf.encode(mm)
  if encoded.len <= 0:
    trace "empty message, skipping"
    return

  logScope:
    encoded = shortLog(encoded)

  let digest = $(sha256.digest(encoded))
  if digest in p.sentRpcCache:
    trace "message already sent to peer, skipping"
    libp2p_pubsub_skipped_sent_messages.inc(labelValues = [p.id])
    return

  try:
    trace "about to send message"
    if p.connected: # this can happen if the remote disconnected
      trace "sending encoded msgs to peer"

      await p.sendConn.writeLp(encoded)
      p.sentRpcCache.put(digest)

      for x in mm.messages:
        for t in x.topicIDs:
          # metrics
          libp2p_pubsub_sent_messages.inc(labelValues = [p.id, t])

  except CatchableError as exc:
    trace "unable to send to remote", exc = exc.msg
    if not(isNil(p.sendConn)):
      await p.sendConn.close()
      p.sendConn = nil
      p.onConnect.clear()

    raise exc

proc sendSubOpts*(p: PubSubPeer, topics: seq[string], subscribe: bool): Future[void] =
  trace "sending subscriptions", peer = p.id, subscribe, topicIDs = topics

  p.send(RPCMsg(
    subscriptions: topics.mapIt(SubOpts(subscribe: subscribe, topic: it))))

proc sendGraft*(p: PubSubPeer, topics: seq[string]): Future[void] =
  trace "sending graft to peer", peer = p.id, topicIDs = topics
  p.send(RPCMsg(control: some(
    ControlMessage(graft: topics.mapIt(ControlGraft(topicID: it))))))

proc sendPrune*(p: PubSubPeer, topics: seq[string]): Future[void] =
  trace "sending prune to peer", peer = p.id, topicIDs = topics
  p.send(RPCMsg(control: some(
    ControlMessage(prune: topics.mapIt(ControlPrune(topicID: it))))))

proc `$`*(p: PubSubPeer): string =
  p.id

proc newPubSubPeer*(peerInfo: PeerInfo,
                    proto: string): PubSubPeer =
  new result
  result.proto = proto
  result.peerInfo = peerInfo
  result.sentRpcCache = newTimedCache[string](2.minutes)
  result.recvdRpcCache = newTimedCache[string](2.minutes)
  result.onConnect = newAsyncEvent()
