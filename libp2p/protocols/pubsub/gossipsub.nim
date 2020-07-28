## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[tables, sets, options, sequtils, random]
import chronos, chronicles, metrics
import pubsub,
       floodsub,
       pubsubpeer,
       peertable,
       mcache,
       timedcache,
       rpc/[messages, message],
       ../protocol,
       ../../peerinfo,
       ../../stream/connection,
       ../../peerid,
       ../../errors,
       ../../utility

logScope:
  topics = "gossipsub"

const GossipSubCodec* = "/meshsub/1.0.0"

# overlay parameters
const GossipSubD* = 6
const GossipSubDlo* = 4
const GossipSubDhi* = 12

# gossip parameters
const GossipSubHistoryLength* = 5
const GossipSubHistoryGossip* = 3

# heartbeat interval
const GossipSubHeartbeatInitialDelay* = 100.millis
const GossipSubHeartbeatInterval* = 1.seconds

# fanout ttl
const GossipSubFanoutTTL* = 1.minutes

type
  GossipSub* = ref object of FloodSub
    mesh*: PeerTable                           # peers that we send messages to when we are subscribed to the topic
    fanout*: PeerTable                         # peers that we send messages to when we're not subscribed to the topic
    gossipsub*: PeerTable                      # peers that are subscribed to a topic
    lastFanoutPubSub*: Table[string, Moment]   # last publish time for fanout topics
    gossip*: Table[string, seq[ControlIHave]]  # pending gossip
    control*: Table[string, ControlMessage]    # pending control messages
    mcache*: MCache                            # messages cache
    heartbeatFut: Future[void]                 # cancellation future for heartbeat interval
    heartbeatRunning: bool
    heartbeatLock: AsyncLock                   # heartbeat lock to prevent two consecutive concurrent heartbeats

declareGauge(libp2p_gossipsub_peers_per_topic_mesh,
  "gossipsub peers per topic in mesh",
  labels = ["topic"])

declareGauge(libp2p_gossipsub_peers_per_topic_fanout,
  "gossipsub peers per topic in fanout",
  labels = ["topic"])

declareGauge(libp2p_gossipsub_peers_per_topic_gossipsub,
  "gossipsub peers per topic in gossipsub",
  labels = ["topic"])

method init*(g: GossipSub) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##

    await g.handleConn(conn, proto)

  g.handler = handler
  g.codec = GossipSubCodec

proc replenishFanout(g: GossipSub, topic: string) =
  ## get fanout peers for a topic
  trace "about to replenish fanout"

  if g.fanout.peers(topic) < GossipSubDLo:
    trace "replenishing fanout", peers = g.fanout.peers(topic)
    if topic in g.gossipsub:
      for peer in g.gossipsub[topic]:
        if g.fanout.addPeer(topic, peer):
          if g.fanout.peers(topic) == GossipSubD:
            break

  libp2p_gossipsub_peers_per_topic_fanout
    .set(g.fanout.peers(topic).int64, labelValues = [topic])

  trace "fanout replenished with peers", peers = g.fanout.peers(topic)

proc rebalanceMesh(g: GossipSub, topic: string) {.async.} =
  logScope:
    topic

  trace "about to rebalance mesh"

  # create a mesh topic that we're subscribing to

  var
    grafts, prunes: seq[PubSubPeer]

  if g.mesh.peers(topic) < GossipSubDlo:
    trace "replenishing mesh", peers = g.mesh.peers(topic)
    # replenish the mesh if we're below Dlo
    grafts = toSeq(
      g.gossipsub.getOrDefault(topic, initHashSet[PubSubPeer]()) -
      g.mesh.getOrDefault(topic, initHashSet[PubSubPeer]())
    )

    logScope:
      meshPeers = g.mesh.peers(topic)
      grafts = grafts.len

    shuffle(grafts)

    # Graft peers so we reach a count of D
    grafts.setLen(min(grafts.len, GossipSubD - g.mesh.peers(topic)))

    trace "getting peers", topic, peers = grafts.len

    for peer in grafts:
      if g.mesh.addPeer(topic, peer):
        g.fanout.removePeer(topic, peer)

  if g.mesh.peers(topic) > GossipSubDhi:
    # prune peers if we've gone over Dhi
    prunes = toSeq(g.mesh[topic])
    shuffle(prunes)
    prunes.setLen(prunes.len - GossipSubD) # .. down to D peers

    trace "about to prune mesh", prunes = prunes.len
    for peer in prunes:
      g.mesh.removePeer(topic, peer)

  libp2p_gossipsub_peers_per_topic_gossipsub
    .set(g.gossipsub.peers(topic).int64, labelValues = [topic])

  libp2p_gossipsub_peers_per_topic_fanout
    .set(g.fanout.peers(topic).int64, labelValues = [topic])

  libp2p_gossipsub_peers_per_topic_mesh
    .set(g.mesh.peers(topic).int64, labelValues = [topic])

  # Send changes to peers after table updates to avoid stale state
  for p in grafts:
    await p.sendGraft(@[topic])
  for p in prunes:
    await p.sendPrune(@[topic])

  trace "mesh balanced, got peers", peers = g.mesh.peers(topic)

proc dropFanoutPeers(g: GossipSub) =
  # drop peers that we haven't published to in
  # GossipSubFanoutTTL seconds
  let now = Moment.now()
  for topic in toSeq(g.lastFanoutPubSub.keys):
    let val = g.lastFanoutPubSub[topic]
    if now > val:
      g.fanout.del(topic)
      g.lastFanoutPubSub.del(topic)
      trace "dropping fanout topic", topic

    libp2p_gossipsub_peers_per_topic_fanout
      .set(g.fanout.peers(topic).int64, labelValues = [topic])

proc getGossipPeers(g: GossipSub): Table[string, ControlMessage] {.gcsafe.} =
  ## gossip iHave messages to peers
  ##

  trace "getting gossip peers (iHave)"
  let topics = toHashSet(toSeq(g.mesh.keys)) + toHashSet(toSeq(g.fanout.keys))
  let controlMsg = ControlMessage()
  for topic in topics:
    var allPeers = toSeq(g.gossipsub.getOrDefault(topic))
    shuffle(allPeers)

    let mesh = g.mesh.getOrDefault(topic)
    let fanout = g.fanout.getOrDefault(topic)

    let gossipPeers = mesh + fanout
    let mids = g.mcache.window(topic)
    if mids.len <= 0:
      continue

    let ihave = ControlIHave(topicID: pbSome(type(ControlIHave.topicID), topic),
                              messageIDs: toSeq(mids))

    if topic notin g.gossipsub:
      trace "topic not in gossip array, skipping", topicID = topic
      continue

    for peer in allPeers:
      if result.len >= GossipSubD:
        trace "got gossip peers", peers = result.len
        break

      if peer in gossipPeers:
        continue

      if peer.id notin result:
        result[peer.id] = controlMsg

      result[peer.id].ihave.add(ihave)

proc heartbeat(g: GossipSub) {.async.} =
  while g.heartbeatRunning:
    try:
      trace "running heartbeat"

      for t in toSeq(g.topics.keys):
        await g.rebalanceMesh(t)

      g.dropFanoutPeers()

      # replenish known topics to the fanout
      for t in toSeq(g.fanout.keys):
        g.replenishFanout(t)

      let peers = g.getGossipPeers()
      var sent: seq[Future[void]]
      for peer in peers.keys:
        if peer in g.peers:
          sent &= g.peers[peer].send(RPCMsg(control: some(peers[peer])))
      checkFutures(await allFinished(sent))

      g.mcache.shift() # shift the cache
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "exception ocurred in gossipsub heartbeat", exc = exc.msg

    await sleepAsync(GossipSubHeartbeatInterval)

method handleDisconnect*(g: GossipSub, peer: PubSubPeer) =
  ## handle peer disconnects
  procCall FloodSub(g).handleDisconnect(peer)
  for t in toSeq(g.gossipsub.keys):
    g.gossipsub.removePeer(t, peer)

    libp2p_gossipsub_peers_per_topic_gossipsub
      .set(g.gossipsub.peers(t).int64, labelValues = [t])

  for t in toSeq(g.mesh.keys):
    g.mesh.removePeer(t, peer)

    libp2p_gossipsub_peers_per_topic_mesh
      .set(g.mesh.peers(t).int64, labelValues = [t])

  for t in toSeq(g.fanout.keys):
    g.fanout.removePeer(t, peer)

    libp2p_gossipsub_peers_per_topic_fanout
      .set(g.fanout.peers(t).int64, labelValues = [t])

method subscribePeer*(p: GossipSub,
                      conn: Connection) =
  procCall PubSub(p).subscribePeer(conn)
  asyncCheck p.handleConn(conn, GossipSubCodec)

method subscribeTopic*(g: GossipSub,
                       topic: string,
                       subscribe: bool,
                       peerId: string) {.gcsafe, async.} =
  await procCall FloodSub(g).subscribeTopic(topic, subscribe, peerId)

  logScope:
    peer = peerId
    topic

  let peer = g.peers.getOrDefault(peerId)
  if peer == nil:
    debug "subscribeTopic on a nil peer!"
    return

  if subscribe:
    trace "peer subscribed to topic"
    # subscribe remote peer to the topic
    discard g.gossipsub.addPeer(topic, peer)
  else:
    trace "peer unsubscribed from topic"
    # unsubscribe remote peer from the topic
    g.gossipsub.removePeer(topic, peer)
    g.mesh.removePeer(topic, peer)
    g.fanout.removePeer(topic, peer)

    libp2p_gossipsub_peers_per_topic_mesh
      .set(g.mesh.peers(topic).int64, labelValues = [topic])
    libp2p_gossipsub_peers_per_topic_fanout
      .set(g.fanout.peers(topic).int64, labelValues = [topic])

  libp2p_gossipsub_peers_per_topic_gossipsub
    .set(g.gossipsub.peers(topic).int64, labelValues = [topic])

  trace "gossip peers", peers = g.gossipsub.peers(topic), topic

  # also rebalance current topic if we are subbed to
  if topic in g.topics:
    await g.rebalanceMesh(topic)

proc handleGraft(g: GossipSub,
                 peer: PubSubPeer,
                 grafts: seq[ControlGraft]): seq[ControlPrune] =
  for graft in grafts:
    let topic = graft.topicID
    logScope:
      peer = peer.id
      topic

    trace "peer grafted topic"

      # If they send us a graft before they send us a subscribe, what should
      # we do? For now, we add them to mesh but don't add them to gossipsub.

    if topic in g.topics:
      if g.mesh.peers(topic) < GossipSubDHi:
        # In the spec, there's no mention of DHi here, but implicitly, a
        # peer will be removed from the mesh on next rebalance, so we don't want
        # this peer to push someone else out
        if g.mesh.addPeer(topic, peer):
          g.fanout.removePeer(topic, peer)
        else:
          trace "peer already in mesh"
      else:
        result.add(ControlPrune(topicID: topic))
    else:
      debug "peer grafting topic we're not interested in"
      result.add(ControlPrune(topicID: topic))

    libp2p_gossipsub_peers_per_topic_mesh
      .set(g.mesh.peers(topic).int64, labelValues = [topic])
    libp2p_gossipsub_peers_per_topic_fanout
      .set(g.fanout.peers(topic).int64, labelValues = [topic])

proc handlePrune(g: GossipSub, peer: PubSubPeer, prunes: seq[ControlPrune]) =
  for prune in prunes:
    trace "peer pruned topic", peer = peer.id, topic = prune.topicID

    g.mesh.removePeer(prune.topicID, peer)
    libp2p_gossipsub_peers_per_topic_mesh
      .set(g.mesh.peers(prune.topicID).int64, labelValues = [prune.topicID])

proc handleIHave(g: GossipSub,
                 peer: PubSubPeer,
                 ihaves: seq[ControlIHave]): ControlIWant =
  for ihave in ihaves:
    trace "peer sent ihave",
      peer = peer.id, topic = ihave.topicID.get(), msgs = ihave.messageIDs

    if ihave.topicID.get() in g.mesh:
      for m in ihave.messageIDs:
        if m notin g.seen:
          result.messageIDs.add(m)

proc handleIWant(g: GossipSub,
                 peer: PubSubPeer,
                 iwants: seq[ControlIWant]): seq[Message] =
  for iwant in iwants:
    for mid in iwant.messageIDs:
      trace "peer sent iwant", peer = peer.id, messageID = mid
      let msg = g.mcache.get(mid)
      if msg.isSome:
        result.add(msg.get())

method rpcHandler*(g: GossipSub,
                  peer: PubSubPeer,
                  rpcMsgs: seq[RPCMsg]) {.async.} =
  await procCall PubSub(g).rpcHandler(peer, rpcMsgs)

  for m in rpcMsgs:                                  # for all RPC messages
    if m.messages.len > 0:                           # if there are any messages
      var toSendPeers: HashSet[PubSubPeer]
      for msg in m.messages:                         # for every message
        let msgId = g.msgIdProvider(msg)
        logScope: msgId

        if msgId in g.seen:
          trace "message already processed, skipping"
          continue

        trace "processing message"

        g.seen.put(msgId)                        # add the message to the seen cache

        if g.verifySignature and not msg.verify(peer.peerInfo):
          trace "dropping message due to failed signature verification"
          continue

        if not (await g.validate(msg)):
          trace "dropping message due to failed validation"
          continue

        # this shouldn't happen
        if g.peerInfo.peerId.data == msg.fromPeer:
          trace "skipping messages from self"
          continue

        for t in msg.topicIDs:                     # for every topic in the message
          if t in g.floodsub:
            toSendPeers.incl(g.floodsub[t])        # get all floodsub peers for topic

          if t in g.mesh:
            toSendPeers.incl(g.mesh[t])            # get all mesh peers for topic

          if t in g.topics:                        # if we're subscribed to the topic
            for h in g.topics[t].handler:
              trace "calling handler for message", topicId = t,
                                                   localPeer = g.peerInfo.id,
                                                   fromPeer = PeerID(data: msg.fromPeer).pretty
              try:
                await h(t, msg.data)                 # trigger user provided handler
              except CatchableError as exc:
                trace "exception in message handler", exc = exc.msg

      # forward the message to all peers interested in it
      let published = await g.publishHelper(toSendPeers, m.messages)

      trace "forwared message to peers", peers = published

    var respControl: ControlMessage
    if m.control.isSome:
      let control = m.control.get()
      g.handlePrune(peer, control.prune)

      respControl.iwant.add(g.handleIHave(peer, control.ihave))
      respControl.prune.add(g.handleGraft(peer, control.graft))
      let messages = g.handleIWant(peer, control.iwant)

      if respControl.graft.len > 0 or respControl.prune.len > 0 or
         respControl.ihave.len > 0 or respControl.iwant.len > 0:
        await peer.send(
          RPCMsg(control: some(respControl), messages: messages))

method subscribe*(g: GossipSub,
                  topic: string,
                  handler: TopicHandler) {.async.} =
  await procCall PubSub(g).subscribe(topic, handler)
  await g.rebalanceMesh(topic)

method unsubscribe*(g: GossipSub,
                    topics: seq[TopicPair]) {.async.} =
  await procCall PubSub(g).unsubscribe(topics)

  for (topic, handler) in topics:
    # delete from mesh only if no handlers are left
    if g.topics[topic].handler.len <= 0:
      if topic in g.mesh:
        let peers = g.mesh.getOrDefault(topic)
        g.mesh.del(topic)

        var pending = newSeq[Future[void]]()
        for peer in peers:
          pending.add(peer.sendPrune(@[topic]))
        checkFutures(await allFinished(pending))

method unsubscribeAll*(g: GossipSub, topic: string) {.async.} =
  await procCall PubSub(g).unsubscribeAll(topic)

  if topic in g.mesh:
    let peers = g.mesh.getOrDefault(topic)
    g.mesh.del(topic)

    var pending = newSeq[Future[void]]()
    for peer in peers:
      pending.add(peer.sendPrune(@[topic]))
    checkFutures(await allFinished(pending))

method publish*(g: GossipSub,
                topic: string,
                data: seq[byte]): Future[int] {.async.} =
  # base returns always 0
  discard await procCall PubSub(g).publish(topic, data)
  trace "publishing message on topic", topic, data = data.shortLog

  var peers: HashSet[PubSubPeer]
  if topic.len <= 0: # data could be 0/empty
    return 0

  if topic in g.topics: # if we're subscribed use the mesh
    peers = g.mesh.getOrDefault(topic)
  else: # not subscribed, send to fanout peers
    # try optimistically
    peers = g.fanout.getOrDefault(topic)
    if peers.len == 0:
      # ok we had nothing.. let's try replenish inline
      g.replenishFanout(topic)
      peers = g.fanout.getOrDefault(topic)

    # even if we couldn't publish,
    # we still attempted to publish
    # on the topic, so it makes sense
    # to update the last topic publish
    # time
    g.lastFanoutPubSub[topic] = Moment.fromNow(GossipSubFanoutTTL)

  inc g.msgSeqno
  let
    msg = Message.init(g.peerInfo, data, topic, g.msgSeqno, g.sign)
    msgId = g.msgIdProvider(msg)

  trace "created new message", msg, topic, peers = peers.len

  if msgId notin g.mcache:
    g.mcache.put(msgId, msg)

  let published = await g.publishHelper(peers, @[msg])
  if published > 0:
    libp2p_pubsub_messages_published.inc(labelValues = [topic])

  trace "published message to peers", peers = published,
                                      msg = msg.shortLog()
  return published

method start*(g: GossipSub) {.async.} =
  trace "gossipsub start"

  ## start pubsub
  ## start long running/repeating procedures

  # interlock start to to avoid overlapping to stops
  await g.heartbeatLock.acquire()

  # setup the heartbeat interval
  g.heartbeatRunning = true
  g.heartbeatFut = g.heartbeat()

  g.heartbeatLock.release()

method stop*(g: GossipSub) {.async.} =
  trace "gossipsub stop"

  ## stop pubsub
  ## stop long running tasks

  await g.heartbeatLock.acquire()

  # stop heartbeat interval
  g.heartbeatRunning = false
  if not g.heartbeatFut.finished:
    trace "awaiting last heartbeat"
    await g.heartbeatFut
    trace "heartbeat stopped"

  g.heartbeatLock.release()

method initPubSub*(g: GossipSub) =
  procCall FloodSub(g).initPubSub()

  randomize()
  g.mcache = newMCache(GossipSubHistoryGossip, GossipSubHistoryLength)
  g.mesh = initTable[string, HashSet[PubSubPeer]]()     # meshes - topic to peer
  g.fanout = initTable[string, HashSet[PubSubPeer]]()   # fanout - topic to peer
  g.gossipsub = initTable[string, HashSet[PubSubPeer]]()# topic to peer map of all gossipsub peers
  g.lastFanoutPubSub = initTable[string, Moment]()  # last publish time for fanout topics
  g.gossip = initTable[string, seq[ControlIHave]]() # pending gossip
  g.control = initTable[string, ControlMessage]()   # pending control messages
  g.heartbeatLock = newAsyncLock()
