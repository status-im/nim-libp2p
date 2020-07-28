## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import sequtils, tables, sets, strutils
import chronos, chronicles, metrics
import pubsub,
       pubsubpeer,
       timedcache,
       peertable,
       rpc/[messages, message],
       ../../stream/connection,
       ../../peerid,
       ../../peerinfo

logScope:
  topics = "floodsub"

const FloodSubCodec* = "/floodsub/1.0.0"

type
  FloodSub* = ref object of PubSub
    floodsub*: PeerTable      # topic to remote peer map
    seen*: TimedCache[string] # list of messages forwarded to peers

method subscribeTopic*(f: FloodSub,
                       topic: string,
                       subscribe: bool,
                       peerId: string) {.gcsafe, async.} =
  await procCall PubSub(f).subscribeTopic(topic, subscribe, peerId)

  let peer = f.peers.getOrDefault(peerId)
  if peer == nil:
    debug "subscribeTopic on a nil peer!"
    return

  if topic notin f.floodsub:
    f.floodsub[topic] = initHashSet[PubSubPeer]()

  if subscribe:
    trace "adding subscription for topic", peer = peer.id, name = topic
    # subscribe the peer to the topic
    f.floodsub[topic].incl(peer)
  else:
    trace "removing subscription for topic", peer = peer.id, name = topic
    # unsubscribe the peer from the topic
    f.floodsub[topic].excl(peer)

method handleDisconnect*(f: FloodSub, peer: PubSubPeer) =
  ## handle peer disconnects
  for t in toSeq(f.floodsub.keys):
    if t in f.floodsub:
      f.floodsub[t].excl(peer)

  procCall PubSub(f).handleDisconnect(peer)

method rpcHandler*(f: FloodSub,
                   peer: PubSubPeer,
                   rpcMsgs: seq[RPCMsg]) {.async.} =
  await procCall PubSub(f).rpcHandler(peer, rpcMsgs)

  for m in rpcMsgs:                                  # for all RPC messages
    if m.messages.len > 0:                           # if there are any messages
      var toSendPeers = initHashSet[PubSubPeer]()
      for msg in m.messages:                         # for every message
        let msgId = f.msgIdProvider(msg)
        logScope: msgId

        if msgId notin f.seen:
          f.seen.put(msgId)                          # add the message to the seen cache

          if f.verifySignature and not msg.verify(peer.peerInfo):
            trace "dropping message due to failed signature verification"
            continue

          if not (await f.validate(msg)):
            trace "dropping message due to failed validation"
            continue

          for t in msg.topicIDs:                     # for every topic in the message
            if t in f.floodsub:
              toSendPeers.incl(f.floodsub[t])        # get all the peers interested in this topic
            if t in f.topics:                        # check that we're subscribed to it
              for h in f.topics[t].handler:
                trace "calling handler for message", topicId = t,
                                                     localPeer = f.peerInfo.id,
                                                     fromPeer = PeerID(data: msg.fromPeer).pretty

                try:
                  await h(t, msg.data)                 # trigger user provided handler
                except CatchableError as exc:
                  trace "exception in message handler", exc = exc.msg

        # forward the message to all peers interested in it
        let published = await f.publishHelper(toSendPeers, m.messages)

        trace "forwared message to peers", peers = published

method init*(f: FloodSub) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##

    await f.handleConn(conn, proto)

  f.handler = handler
  f.codec = FloodSubCodec

method subscribePeer*(p: FloodSub,
                      conn: Connection) =
  procCall PubSub(p).subscribePeer(conn)
  asyncCheck p.handleConn(conn, FloodSubCodec)

method publish*(f: FloodSub,
                topic: string,
                data: seq[byte]): Future[int] {.async.} =
  # base returns always 0
  discard await procCall PubSub(f).publish(topic, data)

  if data.len <= 0 or topic.len <= 0:
    trace "topic or data missing, skipping publish"
    return

  if topic notin f.floodsub:
    trace "missing peers for topic, skipping publish"
    return

  trace "publishing on topic", name = topic
  inc f.msgSeqno
  let msg = Message.init(f.peerInfo, data, topic, f.msgSeqno, f.sign)

  # start the future but do not wait yet
  let published = await f.publishHelper(f.floodsub.getOrDefault(topic), @[msg])

  libp2p_pubsub_messages_published.inc(labelValues = [topic])

  trace "published message to peers", peers = published,
                                      msg = msg.shortLog()
  return published

method unsubscribe*(f: FloodSub,
                    topics: seq[TopicPair]) {.async.} =
  await procCall PubSub(f).unsubscribe(topics)

  for p in f.peers.values:
    await f.sendSubs(p, topics.mapIt(it.topic).deduplicate(), false)

method unsubscribeAll*(f: FloodSub, topic: string) {.async.} =
  await procCall PubSub(f).unsubscribeAll(topic)

  for p in f.peers.values:
    await f.sendSubs(p, @[topic], false)

method initPubSub*(f: FloodSub) =
  procCall PubSub(f).initPubSub()
  f.peers = initTable[string, PubSubPeer]()
  f.topics = initTable[string, Topic]()
  f.floodsub = initTable[string, HashSet[PubSubPeer]]()
  f.seen = newTimedCache[string](2.minutes)
  f.init()
