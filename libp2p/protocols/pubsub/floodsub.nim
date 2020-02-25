## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import sequtils, tables, options, sets, strutils
import chronos, chronicles
import pubsub,
       pubsubpeer,
       ../utils/timedcache,
       rpc/[messages, message],
       ../../crypto/crypto,
       ../../connection,
       ../../peerinfo,
       ../../peer

logScope:
  topic = "FloodSub"

const FloodSubCodec* = "/floodsub/1.0.0"

type
  FloodSub* = ref object of PubSub
    floodsub*: Table[string, HashSet[string]] # topic to remote peer map
    seen*: TimedCache[string]                 # list of messages forwarded to peers

method subscribeTopic*(f: FloodSub,
                       topic: string,
                       subscribe: bool,
                       peerId: string) {.gcsafe.} =
  procCall PubSub(f).subscribeTopic(topic, subscribe, peerId)

  if topic notin f.floodsub:
    f.floodsub[topic] = initHashSet[string]()

  if subscribe:
    trace "adding subscription for topic", peer = peerId, name = topic
    # subscribe the peer to the topic
    f.floodsub[topic].incl(peerId)
  else:
    trace "removing subscription for topic", peer = peerId, name = topic
    # unsubscribe the peer from the topic
    f.floodsub[topic].excl(peerId)

method handleDisconnect*(f: FloodSub, peer: PubSubPeer) {.async.} =
  ## handle peer disconnects
  for t in f.floodsub.keys:
    f.floodsub[t].excl(peer.id)

method rpcHandler*(f: FloodSub,
                   peer: PubSubPeer,
                   rpcMsgs: seq[RPCMsg]) {.async.} =
  await procCall PubSub(f).rpcHandler(peer, rpcMsgs)

  for m in rpcMsgs:                                  # for all RPC messages
    if m.messages.len > 0:                           # if there are any messages
      var toSendPeers: HashSet[string] = initHashSet[string]()
      for msg in m.messages:                         # for every message
        if msg.msgId notin f.seen:
          f.seen.put(msg.msgId)                      # add the message to the seen cache

          if not msg.verify(peer.peerInfo):
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
                trace "calling handler for message", msg = msg.msgId,
                                                     topicId = t,
                                                     localPeer = f.peerInfo.id,
                                                     fromPeer = msg.fromPeerId().pretty
                await h(t, msg.data)                 # trigger user provided handler

        # forward the message to all peers interested in it
        var sent: seq[Future[void]]
        for p in toSendPeers:
          if p in f.peers and f.peers[p].id != peer.id:
            sent.add(f.peers[p].send(@[RPCMsg(messages: m.messages)]))
        await allFutures(sent)

method init(f: FloodSub) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##

    await f.handleConn(conn, proto)

  f.handler = handler
  f.codec = FloodSubCodec

method publish*(f: FloodSub,
                topic: string,
                data: seq[byte]) {.async.} =
  await procCall PubSub(f).publish(topic, data)

  if data.len <= 0 or topic.len <= 0:
    trace "topic or data missing, skipping publish"
    return

  if topic notin f.floodsub:
    trace "missing peers for topic, skipping publish"
    return

  trace "publishing on topic", name = topic
  let msg = newMessage(f.peerInfo, data, topic)
  var sent: seq[Future[void]]
  for p in f.floodsub[topic]:
    trace "publishing message", name = topic, peer = p, data = data
    sent.add(f.peers[p].send(@[RPCMsg(messages: @[msg])]))
  await allFutures(sent)

method unsubscribe*(f: FloodSub,
                    topics: seq[TopicPair]) {.async.} =
  await procCall PubSub(f).unsubscribe(topics)

  for p in f.peers.values:
    await f.sendSubs(p, topics.mapIt(it.topic).deduplicate(), false)

method initPubSub*(f: FloodSub) =
  f.peers = initTable[string, PubSubPeer]()
  f.topics = initTable[string, Topic]()
  f.floodsub = initTable[string, HashSet[string]]()
  f.seen = newTimedCache[string](2.minutes)
  f.init()
