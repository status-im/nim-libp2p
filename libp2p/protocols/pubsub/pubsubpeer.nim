## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[sequtils, strutils, tables, hashes]
import chronos, chronicles, nimcrypto/sha2, metrics
import rpc/[messages, message, protobuf],
       ../../peerid,
       ../../peerinfo,
       ../../stream/connection,
       ../../crypto/crypto,
       ../../protobuf/minprotobuf,
       ../../utility,
       ../../switch

export peerid, connection

logScope:
  topics = "libp2p pubsubpeer"

when defined(libp2p_expensive_metrics):
  declareCounter(libp2p_pubsub_sent_messages, "number of messages sent", labels = ["id", "topic"])
  declareCounter(libp2p_pubsub_received_messages, "number of messages received", labels = ["id", "topic"])
  declareCounter(libp2p_pubsub_skipped_received_messages, "number of received skipped messages", labels = ["id"])
  declareCounter(libp2p_pubsub_skipped_sent_messages, "number of sent skipped messages", labels = ["id"])

type
  PubSubObserver* = ref object
    onRecv*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}
    onSend*: proc(peer: PubSubPeer; msgs: var RPCMsg) {.gcsafe, raises: [Defect].}

  PubSubPeerEventKind* {.pure.} = enum
    Connected
    Disconnected

  PubsubPeerEvent* = object
    kind*: PubSubPeerEventKind

  GetConn* = proc(): Future[Connection] {.gcsafe, raises: [Defect, DialFailedError].}
  DropConn* = proc(peer: PubsubPeer) {.gcsafe, raises: [Defect].} # have to pass peer as it's unknown during init
  OnEvent* = proc(peer: PubSubPeer, event: PubsubPeerEvent) {.gcsafe, raises: [Defect].}

  PubSubPeer* = ref object of RootObj
    getConn*: GetConn                   # callback to establish a new send connection
    dropConn*: DropConn                 # Function pointer to use to drop connections
    onEvent*: OnEvent                   # Connectivity updates for peer
    codec*: string                      # the protocol that this peer joined from
    sendConn*: Connection               # cached send connection
    address*: Option[MultiAddress]
    peerId*: PeerID
    handler*: RPCHandler
    observers*: ref seq[PubSubObserver] # ref as in smart_ptr

    score*: float64
    iWantBudget*: int
    iHaveBudget*: int
    appScore*: float64 # application specific score
    behaviourPenalty*: float64 # the eventual penalty score

    when defined(libp2p_agents_metrics):
      shortAgent*: string

  RPCHandler* = proc(peer: PubSubPeer, msg: RPCMsg): Future[void]
    {.gcsafe, raises: [Defect].}

func hash*(p: PubSubPeer): Hash =
  p.peerId.hash

func `==`*(a, b: PubSubPeer): bool =
  a.peerId == b.peerId

func shortLog*(p: PubSubPeer): string =
  if p.isNil: "PubSubPeer(nil)"
  else: shortLog(p.peerId)
chronicles.formatIt(PubSubPeer): shortLog(it)

proc connected*(p: PubSubPeer): bool =
  not p.sendConn.isNil and not
    (p.sendConn.closed or p.sendConn.atEof)

proc hasObservers(p: PubSubPeer): bool =
  p.observers != nil and anyIt(p.observers[], it != nil)

func outbound*(p: PubSubPeer): bool =
  # gossipsub 1.1 spec requires us to know if the transport is outgoing
  # in order to give priotity to connections we make
  # https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#outbound-mesh-quotas
  # This behaviour is presrcibed to counter sybil attacks and ensures that a coordinated inbound attack can never fully take over the mesh
  if not p.sendConn.isNil and p.sendConn.transportDir == Direction.Out:
    true
  else:
    false

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
  debug "starting pubsub read loop",
    conn, peer = p, closed = conn.closed
  try:
    try:
      while not conn.atEof:
        trace "waiting for data", conn, peer = p, closed = conn.closed

        let data = await conn.readLp(64 * 1024)
        trace "read data from peer",
          conn, peer = p, closed = conn.closed,
          data = data.shortLog

        var rmsg = decodeRpcMsg(data)
        if rmsg.isErr():
          notice "failed to decode msg from peer",
            conn, peer = p, closed = conn.closed,
            err = rmsg.error()
          break

        trace "decoded msg from peer",
          conn, peer = p, closed = conn.closed,
          msg = rmsg.get().shortLog
        # trigger hooks
        p.recvObservers(rmsg.get())

        when defined(libp2p_expensive_metrics):
          for m in rmsg.get().messages:
            for t in m.topicIDs:
              # metrics
              libp2p_pubsub_received_messages.inc(labelValues = [$p.peerId, t])

        await p.handler(p, rmsg.get())
    finally:
      await conn.close()
  except CancelledError:
    # This is top-level procedure which will work as separate task, so it
    # do not need to propagate CancelledError.
    trace "Unexpected cancellation in PubSubPeer.handle"
  except CatchableError as exc:
    trace "Exception occurred in PubSubPeer.handle",
      conn, peer = p, closed = conn.closed, exc = exc.msg
  finally:
    debug "exiting pubsub read loop",
      conn, peer = p, closed = conn.closed

proc connectOnce(p: PubSubPeer): Future[void] {.async.} =
  try:
    let newConn = await p.getConn()
    if newConn.isNil:
      raise (ref LPError)(msg: "Cannot establish send connection")

    # When the send channel goes up, subscriptions need to be sent to the
    # remote peer - if we had multiple channels up and one goes down, all
    # stop working so we make an effort to only keep a single channel alive

    trace "Get new send connection", p, newConn
    p.sendConn = newConn
    p.address = some(p.sendConn.observedAddr)

    if p.onEvent != nil:
      p.onEvent(p, PubsubPeerEvent(kind: PubSubPeerEventKind.Connected))

    await handle(p, newConn)
  finally:
    if p.sendConn != nil:
      trace "Removing send connection", p, conn = p.sendConn
      await p.sendConn.close()
      p.sendConn = nil

    try:
      if p.onEvent != nil:
        p.onEvent(p, PubsubPeerEvent(kind: PubSubPeerEventKind.Disconnected))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "Errors during diconnection events", error = exc.msg

    # don't cleanup p.address else we leak some gossip stat table

proc connectImpl(p: PubSubPeer) {.async.} =
  try:
    # Keep trying to establish a connection while it's possible to do so - the
    # send connection might get disconnected due to a timeout or an unrelated
    # issue so we try to get a new on
    while true:
      await connectOnce(p)
  except CatchableError as exc: # never cancelled
    debug "Could not establish send connection", msg = exc.msg
  finally:
    # drop the connection, else we end up with ghost peers
    if p.dropConn != nil: p.dropConn(p)

proc connect*(p: PubSubPeer) =
  asyncSpawn connectImpl(p)

proc sendImpl(conn: Connection, encoded: seq[byte]) {.async.} =
  try:
    trace "sending encoded msgs to peer", conn, encoded = shortLog(encoded)
    await conn.writeLp(encoded)
    trace "sent pubsub message to remote", conn

  except CatchableError as exc: # never cancelled
    # Because we detach the send call from the currently executing task using
    # asyncSpawn, no exceptions may leak out of it
    trace "Unable to send to remote", conn, msg = exc.msg
    # Next time sendConn is used, it will be have its close flag set and thus
    # will be recycled

    await conn.close() # This will clean up the send connection

template sendMetrics(msg: RPCMsg): untyped =
  when defined(libp2p_expensive_metrics):
    for x in msg.messages:
      for t in x.topicIDs:
        # metrics
        libp2p_pubsub_sent_messages.inc(labelValues = [$p.peerId, t])

proc send*(p: PubSubPeer, msg: RPCMsg, anonymize: bool) {.raises: [Defect].} =
  doAssert(not isNil(p), "pubsubpeer nil!")

  let conn = p.sendConn
  if conn == nil or conn.closed():
    trace "No send connection, skipping message", p, msg
    return

  trace "sending msg to peer", peer = p, rpcMsg = shortLog(msg)

  # When sending messages, we take care to re-encode them with the right
  # anonymization flag to ensure that we're not penalized for sending invalid
  # or malicious data on the wire - in particular, re-encoding protects against
  # some forms of valid but redundantly encoded protobufs with unknown or
  # duplicated fields
  let encoded = if p.hasObservers():
    var mm = msg
    # trigger send hooks
    p.sendObservers(mm)
    sendMetrics(mm)
    encodeRpcMsg(mm, anonymize)
  else:
    # If there are no send hooks, we redundantly re-encode the message to
    # protobuf for every peer - this could easily be improved!
    sendMetrics(msg)
    encodeRpcMsg(msg, anonymize)

  if encoded.len <= 0:
    debug "empty message, skipping", p, msg
    return

  # To limit the size of the closure, we only pass the encoded message and
  # connection to the spawned send task
  asyncSpawn(try:
    sendImpl(conn, encoded)
  except Exception as exc: # TODO chronos Exception
    raiseAssert exc.msg)

proc newPubSubPeer*(
  peerId: PeerID,
  getConn: GetConn,
  dropConn: DropConn,
  onEvent: OnEvent,
  codec: string): PubSubPeer =

  PubSubPeer(
    getConn: getConn,
    dropConn: dropConn,
    onEvent: onEvent,
    codec: codec,
    peerId: peerId,
  )
