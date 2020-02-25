## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options, hashes, strutils, tables, hashes
import chronos, chronicles
import rpc/[messages, message, protobuf],
       ../utils/timedcache,
       ../../peer,
       ../../peerinfo,
       ../../connection,
       ../../stream/lpstream,
       ../../crypto/crypto,
       ../../protobuf/minprotobuf

logScope:
  topic = "PubSubPeer"

type
    PubSubPeer* = ref object of RootObj
      proto: string # the protocol that this peer joined from
      sendConn: Connection
      peerInfo*: PeerInfo
      handler*: RPCHandler
      topics*: seq[string]
      sentRpcCache: TimedCache[string] # cache for already sent messages
      recvdRpcCache: TimedCache[string] # cache for already received messages
      refs*: int # refcount of the connections this peer is handling
      onConnect: AsyncEvent

    RPCHandler* = proc(peer: PubSubPeer, msg: seq[RPCMsg]): Future[void] {.gcsafe.}

proc id*(p: PubSubPeer): string = p.peerInfo.id

proc isConnected*(p: PubSubPeer): bool =
  (not isNil(p.sendConn))

proc `conn=`*(p: PubSubPeer, conn: Connection) =
  trace "attaching send connection for peer", peer = p.id
  p.sendConn = conn
  p.onConnect.fire()

proc handle*(p: PubSubPeer, conn: Connection) {.async.} =
  trace "handling pubsub rpc", peer = p.id, closed = conn.closed
  try:
    while not conn.closed:
      trace "waiting for data", peer = p.id, closed = conn.closed
      let data = await conn.readLp()
      let hexData = data.toHex()
      trace "read data from peer", peer = p.id, data = hexData
      if $hexData.hash in p.recvdRpcCache:
        trace "message already received, skipping", peer = p.id
        continue

      let msg = decodeRpcMsg(data)
      trace "decoded msg from peer", peer = p.id, msg = $msg
      await p.handler(p, @[msg])
      p.recvdRpcCache.put($hexData.hash)
  except CatchableError as exc:
    error "exception occurred in PubSubPeer.handle", exc = exc.msg
  finally:
    trace "exiting pubsub peer read loop", peer = p.id
    if not conn.closed():
      await conn.close()

proc send*(p: PubSubPeer, msgs: seq[RPCMsg]) {.async.} =
  try:
    for m in msgs:
      trace "sending msgs to peer", toPeer = p.id
      let encoded = encodeRpcMsg(m)
      let encodedHex = encoded.buffer.toHex()
      if encoded.buffer.len <= 0:
        trace "empty message, skipping", peer = p.id
        return

      if $encodedHex.hash in p.sentRpcCache:
        trace "message already sent to peer, skipping", peer = p.id
        continue

      proc sendToRemote() {.async.} =
        trace "sending encoded msgs to peer", peer = p.id, encoded = encodedHex
        await p.sendConn.writeLp(encoded.buffer)
        p.sentRpcCache.put($encodedHex.hash)

      # if no connection has been set,
      # queue messages untill a connection
      # becomes available
      if p.isConnected:
        await sendToRemote()
        return

      p.onConnect.wait().addCallback do (udata: pointer):
          asyncCheck sendToRemote()
      trace "enqueued message to send at a later time", peer = p.id,
                                                        encoded = encodedHex

  except CatchableError as exc:
    trace "exception occurred in PubSubPeer.send", exc = exc.msg

proc sendMsg*(p: PubSubPeer,
              peerId: PeerID,
              topic: string,
              data: seq[byte]): Future[void] {.gcsafe.} =
  p.send(@[RPCMsg(messages: @[newMessage(p.peerInfo, data, topic)])])

proc sendGraft*(p: PubSubPeer, topics: seq[string]) {.async.} =
  for topic in topics:
    trace "sending graft msg to peer", peer = p.id, topicID = topic
    await p.send(@[RPCMsg(control: some(ControlMessage(graft: @[ControlGraft(topicID: topic)])))])

proc sendPrune*(p: PubSubPeer, topics: seq[string]) {.async.} =
  for topic in topics:
    trace "sending prune msg to peer", peer = p.id, topicID = topic
    await p.send(@[RPCMsg(control: some(ControlMessage(prune: @[ControlPrune(topicID: topic)])))])

proc newPubSubPeer*(peerInfo: PeerInfo,
                    proto: string): PubSubPeer =
  new result
  result.proto = proto
  result.peerInfo = peerInfo
  result.sentRpcCache = newTimedCache[string](2.minutes)
  result.recvdRpcCache = newTimedCache[string](2.minutes)
  result.onConnect = newAsyncEvent()
