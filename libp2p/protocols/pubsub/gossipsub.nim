## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import tables, sets, options, sequtils, random
import chronos, chronicles
import pubsub,
       floodsub,
       pubsubpeer,
       mcache,
       rpc/[messages, message],
       ../utils/timedcache,
       ../../crypto/crypto,
       ../protocol,
       ../../peerinfo,
       ../../connection,
       ../../peer

logScope:
  topic = "GossipSub"

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
    mesh*: Table[string, HashSet[string]]      # meshes - topic to peer
    fanout*: Table[string, HashSet[string]]    # fanout - topic to peer
    gossipsub*: Table[string, HashSet[string]] # topic to peer map of all gossipsub peers
    lastFanoutPubSub*: Table[string, Moment]   # last publish time for fanout topics
    gossip*: Table[string, seq[ControlIHave]]  # pending gossip
    control*: Table[string, ControlMessage]    # pending control messages
    mcache*: MCache                            # messages cache
    heartbeatCancel*: Future[void]             # cancelation future for heartbeat interval
    heartbeatLock: AsyncLock                   # hearbeat lock to prevent two concecutive concurent hearbeats

# TODO: This belong in chronos, temporary left here until chronos is updated
proc addInterval(every: Duration, cb: CallbackFunc,
    udata: pointer = nil): Future[void] =
  ## Arrange the callback ``cb`` to be called on every ``Duration`` window

  var retFuture = newFuture[void]("chronos.addInterval(Duration)")
  proc interval(arg: pointer = nil) {.gcsafe.}
  proc scheduleNext() =
    if not retFuture.finished():
      addTimer(Moment.fromNow(every), interval)

  proc interval(arg: pointer = nil) {.gcsafe.} =
    cb(udata)
    scheduleNext()

  scheduleNext()
  return retFuture

method init(g: GossipSub) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##

    await g.handleConn(conn, proto)

  g.handler = handler
  g.codec = GossipSubCodec

method handleDisconnect(g: GossipSub, peer: PubSubPeer) {.async.} =
  ## handle peer disconnects
  await procCall FloodSub(g).handleDisconnect(peer)
  for t in g.gossipsub.keys:
    g.gossipsub[t].excl(peer.id)

  for t in g.mesh.keys:
    g.mesh[t].excl(peer.id)

  for t in g.fanout.keys:
    g.fanout[t].excl(peer.id)

method subscribeTopic*(g: GossipSub,
                       topic: string,
                       subscribe: bool,
                       peerId: string) {.gcsafe.} =
  procCall PubSub(g).subscribeTopic(topic, subscribe, peerId)

  if topic notin g.gossipsub:
    g.gossipsub[topic] = initHashSet[string]()

  if subscribe:
    trace "adding subscription for topic", peer = peerId, name = topic
    # subscribe the peer to the topic
    g.gossipsub[topic].incl(peerId)
  else:
    trace "removing subscription for topic", peer = peerId, name = topic
    # unsubscribe the peer from the topic
    g.gossipsub[topic].excl(peerId)

proc handleGraft(g: GossipSub,
                 peer: PubSubPeer,
                 grafts: seq[ControlGraft],
                 respControl: var ControlMessage) =
  for graft in grafts:
    trace "processing graft message", peer = peer.id,
                                      topicID = graft.topicID

    if graft.topicID in g.topics:
      if g.mesh.len < GossipSubD:
        g.mesh[graft.topicID].incl(peer.id)
      else:
        g.gossipsub[graft.topicID].incl(peer.id)
    else:
      respControl.prune.add(ControlPrune(topicID: graft.topicID))

proc handlePrune(g: GossipSub, peer: PubSubPeer, prunes: seq[ControlPrune]) =
  for prune in prunes:
    trace "processing prune message", peer = peer.id,
                                      topicID = prune.topicID

    if prune.topicID in g.mesh:
      g.mesh[prune.topicID].excl(peer.id)

proc handleIHave(g: GossipSub, peer: PubSubPeer, ihaves: seq[
    ControlIHave]): ControlIWant =
  for ihave in ihaves:
    trace "processing ihave message", peer = peer.id,
                                      topicID = ihave.topicID

    if ihave.topicID in g.mesh:
      for m in ihave.messageIDs:
        if m notin g.seen:
          result.messageIDs.add(m)

proc handleIWant(g: GossipSub, peer: PubSubPeer, iwants: seq[
    ControlIWant]): seq[Message] =
  for iwant in iwants:
    for mid in iwant.messageIDs:
      trace "processing iwant message", peer = peer.id,
                                        messageID = mid
      let msg = g.mcache.get(mid)
      if msg.isSome:
        result.add(msg.get())

method rpcHandler(g: GossipSub,
                  peer: PubSubPeer,
                  rpcMsgs: seq[RPCMsg]) {.async.} =
  await procCall PubSub(g).rpcHandler(peer, rpcMsgs)

  for m in rpcMsgs:                                  # for all RPC messages
    if m.messages.len > 0:                           # if there are any messages
      var toSendPeers: HashSet[string] = initHashSet[string]()
      for msg in m.messages:                         # for every message
        trace "processing message with id", msg = msg.msgId
        if msg.msgId in g.seen:
          trace "message already processed, skipping", msg = msg.msgId
          continue

        g.seen.put(msg.msgId)                        # add the message to the seen cache

        if not msg.verify(peer.peerInfo):
          trace "dropping message due to failed signature verification"
          continue

        if not (await g.validate(msg)):
          trace "dropping message due to failed validation"
          continue

        # this shouldn't happen
        if g.peerInfo.peerId == msg.fromPeerId():
          trace "skipping messages from self", msg = msg.msgId
          continue

        for t in msg.topicIDs:                     # for every topic in the message

          if t in g.floodsub:
            toSendPeers.incl(g.floodsub[t])        # get all floodsub peers for topic

          if t in g.mesh:
            toSendPeers.incl(g.mesh[t])            # get all mesh peers for topic

          if t in g.topics:                        # if we're subscribed to the topic
            for h in g.topics[t].handler:
              trace "calling handler for message", msg = msg.msgId,
                                                   topicId = t,
                                                   localPeer = g.peerInfo.id,
                                                   fromPeer = msg.fromPeerId().pretty
              await h(t, msg.data)                 # trigger user provided handler

      # forward the message to all peers interested in it
      for p in toSendPeers:
        if p in g.peers:
          let id = g.peers[p].peerInfo.peerId
          trace "about to forward message to peer", peerId = id

          if id != peer.peerInfo.peerId:
            let msgs = m.messages.filterIt(
              # don't forward to message originator
              id != it.fromPeerId()
            )

            var sent: seq[Future[void]]
            if msgs.len > 0:
              trace "forwarding message to", peerId = id
              sent.add(g.peers[p].send(@[RPCMsg(messages: msgs)]))
            await allFutures(sent)

    var respControl: ControlMessage
    if m.control.isSome:
      var control: ControlMessage = m.control.get()
      let iWant: ControlIWant = g.handleIHave(peer, control.ihave)
      if iWant.messageIDs.len > 0:
        respControl.iwant.add(iWant)
      let messages: seq[Message] = g.handleIWant(peer, control.iwant)

      g.handleGraft(peer, control.graft, respControl)
      g.handlePrune(peer, control.prune)

      if respControl.graft.len > 0 or respControl.prune.len > 0 or
         respControl.ihave.len > 0 or respControl.iwant.len > 0:
        await peer.send(@[RPCMsg(control: some(respControl), messages: messages)])

proc replenishFanout(g: GossipSub, topic: string) {.async.} =
  ## get fanout peers for a topic
  trace "about to replenish fanout"
  if topic notin g.fanout:
    g.fanout[topic] = initHashSet[string]()

  if g.fanout[topic].len < GossipSubDLo:
    trace "replenishing fanout", peers = g.fanout[topic].len
    if topic in g.gossipsub:
      for p in g.gossipsub[topic]:
        if not g.fanout[topic].containsOrIncl(p):
          if g.fanout[topic].len == GossipSubD:
            break

  trace "fanout replenished with peers", peers = g.fanout[topic].len

proc rebalanceMesh(g: GossipSub, topic: string) {.async.} =
  trace "about to rebalance mesh"
  # create a mesh topic that we're subscribing to
  if topic notin g.mesh:
    g.mesh[topic] = initHashSet[string]()

  if g.mesh[topic].len < GossipSubDlo:
    trace "replenishing mesh"
    # replenish the mesh if we're bellow GossipSubDlo
    while g.mesh[topic].len < GossipSubD:
      trace "gattering peers", peers = g.mesh[topic].len
      var id: string
      if topic in g.fanout and g.fanout[topic].len > 0:
        id = g.fanout[topic].pop()
        trace "got fanout peer", peer = id
      elif topic in g.gossipsub and g.gossipsub[topic].len > 0:
        id = g.gossipsub[topic].pop()
        trace "got gossipsub peer", peer = id
      else:
        trace "no more peers"
        break

      g.mesh[topic].incl(id)
      if id in g.peers:
        let p = g.peers[id]
        # send a graft message to the peer
        await p.sendGraft(@[topic])

  # prune peers if we've gone over
  if g.mesh[topic].len > GossipSubDhi:
    trace "pruning mesh"
    while g.mesh[topic].len > GossipSubD:
      trace "pruning peers", peers = g.mesh[topic].len
      let id = toSeq(g.mesh[topic])[rand(0..<g.mesh[topic].len)]
      g.mesh[topic].excl(id)

      let p = g.peers[id]
      # send a graft message to the peer
      await p.sendPrune(@[topic])

  trace "mesh balanced, got peers", peers = g.mesh[topic].len

proc dropFanoutPeers(g: GossipSub) {.async.} =
  # drop peers that we haven't published to in
  # GossipSubFanoutTTL seconds
  for topic in g.lastFanoutPubSub.keys:
    if Moment.now > g.lastFanoutPubSub[topic]:
      g.lastFanoutPubSub.del(topic)
      g.fanout.del(topic)

proc getGossipPeers(g: GossipSub): Table[string, ControlMessage] {.gcsafe.} =
  ## gossip iHave messages to peers
  let topics = toHashSet(toSeq(g.mesh.keys)) + toHashSet(toSeq(g.fanout.keys))

  for topic in topics:
    let mesh: HashSet[string] =
      if topic in g.mesh:
        g.mesh[topic]
      else:
        initHashSet[string]()

    let fanout: HashSet[string] =
      if topic in g.fanout:
        g.fanout[topic]
      else:
        initHashSet[string]()

    let gossipPeers = mesh + fanout
    let mids = g.mcache.window(topic)
    let ihave = ControlIHave(topicID: topic,
                             messageIDs: toSeq(mids))

    if topic notin g.gossipsub:
      trace "topic not in gossip array, skipping", topicID = topic
      continue

    while result.len < GossipSubD:
      if not (g.gossipsub[topic].len > 0):
        trace "no peers for topic, skipping", topicID = topic
        break

      let id = toSeq(g.gossipsub[topic]).sample()
      g.gossipsub[topic].excl(id)
      if id notin gossipPeers:
        if id notin result:
          result[id] = ControlMessage()
        result[id].ihave.add(ihave)

proc heartbeat(g: GossipSub) {.async.} =
  await g.heartbeatLock.acquire()
  trace "running heartbeat"

  await sleepAsync(GossipSubHeartbeatInitialDelay)

  for t in g.mesh.keys:
    await g.rebalanceMesh(t)

  await g.dropFanoutPeers()
  let peers = g.getGossipPeers()
  for peer in peers.keys:
    await g.peers[peer].send(@[RPCMsg(control: some(peers[peer]))])

  g.mcache.shift() # shift the cache
  g.heartbeatLock.release()

method subscribe*(g: GossipSub,
                  topic: string,
                  handler: TopicHandler) {.async.} =
  await procCall PubSub(g).subscribe(topic, handler)
  asyncCheck g.rebalanceMesh(topic)

method unsubscribe*(g: GossipSub,
                    topics: seq[TopicPair]) {.async.} =
  await procCall PubSub(g).unsubscribe(topics)

  for pair in topics:
    let topic = pair.topic
    if topic in g.mesh:
      let peers = g.mesh[topic]
      g.mesh.del(topic)
      for id in peers:
        let p = g.peers[id]
        await p.sendPrune(@[topic])

method publish*(g: GossipSub,
                topic: string,
                data: seq[byte]) {.async.} =
  await procCall PubSub(g).publish(topic, data)

  trace "about to publish message on topic", name = topic,
                                             data = data.toHex()
  if data.len > 0 and topic.len > 0:
    var peers: HashSet[string]
    if topic in g.topics: # if we're subscribed to the topic attempt to build a mesh
      await g.rebalanceMesh(topic)
      peers = g.mesh[topic]
    else: # send to fanout peers
      await g.replenishFanout(topic)
      if topic in g.fanout:
        peers = g.fanout[topic]
        # set the fanout expiery time
        g.lastFanoutPubSub[topic] = Moment.fromNow(GossipSubFanoutTTL)

    let msg = newMessage(g.peerInfo, data, topic)
    var sent: seq[Future[void]]
    for p in peers:
      if p == g.peerInfo.id:
        continue

      trace "publishing on topic", name = topic
      g.mcache.put(msg)
      sent.add(g.peers[p].send(@[RPCMsg(messages: @[msg])]))
    await allFutures(sent)

method start*(g: GossipSub) {.async.} =
  ## start pubsub
  ## start long running/repeating procedures

  # setup the heartbeat interval
  g.heartbeatCancel = addInterval(GossipSubHeartbeatInterval,
                                  proc (arg: pointer = nil)
                                    {.gcsafe, locks: 0.} =
                                    asyncCheck g.heartbeat)

method stop*(g: GossipSub) {.async.} =
  ## stopt pubsub
  ## stop long running tasks

  await g.heartbeatLock.acquire()

  # stop heartbeat interval
  if not g.heartbeatCancel.finished:
    g.heartbeatCancel.complete()

  g.heartbeatLock.release()

method initPubSub(g: GossipSub) =
  procCall FloodSub(g).initPubSub()

  g.mcache = newMCache(GossipSubHistoryGossip, GossipSubHistoryLength)
  g.mesh = initTable[string, HashSet[string]]() # meshes - topic to peer
  g.fanout = initTable[string, HashSet[string]]() # fanout - topic to peer
  g.gossipsub = initTable[string, HashSet[string]]() # topic to peer map of all gossipsub peers
  g.lastFanoutPubSub = initTable[string, Moment]() # last publish time for fanout topics
  g.gossip = initTable[string, seq[ControlIHave]]() # pending gossip
  g.control = initTable[string, ControlMessage]() # pending control messages
  g.heartbeatLock = newAsyncLock()

## Unit tests
when isMainModule and not defined(release):
  ## Test internal (private) methods for gossip,
  ## mesh and fanout maintenance.
  ## Usually I wouldn't test private behaviour,
  ## but the maintenance methods are quite involved,
  ## hence these tests are here.
  ##

  import unittest
  import ../../stream/bufferstream

  type
    TestGossipSub = ref object of GossipSub

  suite "GossipSub":
    test "`rebalanceMesh` Degree Lo":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        let topic = "foobar"
        gossipSub.mesh[topic] = initHashSet[string]()
        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        for i in 0..<15:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].conn = conn
          gossipSub.mesh[topic].incl(peerInfo.id)

        check gossipSub.peers.len == 15
        await gossipSub.rebalanceMesh(topic)
        check gossipSub.mesh[topic].len == GossipSubD

        result = true

      check:
        waitFor(testRun()) == true

    test "`rebalanceMesh` Degree Hi":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        let topic = "foobar"
        gossipSub.gossipsub[topic] = initHashSet[string]()
        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        for i in 0..<15:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].conn = conn
          gossipSub.gossipsub[topic].incl(peerInfo.id)

        check gossipSub.gossipsub[topic].len == 15
        await gossipSub.rebalanceMesh(topic)
        check gossipSub.mesh[topic].len == GossipSubD

        result = true

      check:
        waitFor(testRun()) == true

    test "`replenishFanout` Degree Lo":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.gossipsub[topic] = initHashSet[string]()
        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        for i in 0..<15:
          let conn = newConnection(newBufferStream(writeHandler))
          var peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          gossipSub.gossipsub[topic].incl(peerInfo.id)

        check gossipSub.gossipsub[topic].len == 15
        await gossipSub.replenishFanout(topic)
        check gossipSub.fanout[topic].len == GossipSubD

        result = true

      check:
        waitFor(testRun()) == true

    test "`dropFanoutPeers` drop expired fanout topics":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.fanout[topic] = initHashSet[string]()
        gossipSub.lastFanoutPubSub[topic] = Moment.fromNow(100.millis)
        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        for i in 0..<6:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          gossipSub.fanout[topic].incl(peerInfo.id)

        check gossipSub.fanout[topic].len == GossipSubD

        await gossipSub.dropFanoutPeers()
        check topic notin gossipSub.fanout

        result = true

      check:
        waitFor(testRun()) == true

    test "`dropFanoutPeers` leave unexpired fanout topics":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        let topic1 = "foobar1"
        let topic2 = "foobar2"
        gossipSub.fanout[topic1] = initHashSet[string]()
        gossipSub.fanout[topic2] = initHashSet[string]()
        gossipSub.lastFanoutPubSub[topic1] = Moment.fromNow(100.millis)
        gossipSub.lastFanoutPubSub[topic1] = Moment.fromNow(500.millis)

        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        for i in 0..<6:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          gossipSub.fanout[topic1].incl(peerInfo.id)
          gossipSub.fanout[topic2].incl(peerInfo.id)

        check gossipSub.fanout[topic1].len == GossipSubD
        check gossipSub.fanout[topic2].len == GossipSubD

        await gossipSub.dropFanoutPeers()
        check topic1 notin gossipSub.fanout
        check topic2 in gossipSub.fanout

        result = true

      check:
        waitFor(testRun()) == true

    test "`getGossipPeers` - should gather up to degree D non intersecting peers":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.mesh[topic] = initHashSet[string]()
        gossipSub.fanout[topic] = initHashSet[string]()
        gossipSub.gossipsub[topic] = initHashSet[string]()
        for i in 0..<30:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          if i mod 2 == 0:
            gossipSub.fanout[topic].incl(peerInfo.id)
          else:
            gossipSub.mesh[topic].incl(peerInfo.id)

        for i in 0..<15:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          gossipSub.gossipsub[topic].incl(peerInfo.id)

        check gossipSub.fanout[topic].len == 15
        check gossipSub.fanout[topic].len == 15
        check gossipSub.gossipsub[topic].len == 15

        let peers = gossipSub.getGossipPeers()
        check peers.len == GossipSubD
        for p in peers.keys:
          check p notin gossipSub.fanout[topic]
          check p notin gossipSub.mesh[topic]

        result = true

      check:
        waitFor(testRun()) == true

    test "`getGossipPeers` - should not crash on missing topics in mesh":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.fanout[topic] = initHashSet[string]()
        gossipSub.gossipsub[topic] = initHashSet[string]()
        for i in 0..<30:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          if i mod 2 == 0:
            gossipSub.fanout[topic].incl(peerInfo.id)
          else:
            gossipSub.gossipsub[topic].incl(peerInfo.id)

        let peers = gossipSub.getGossipPeers()
        check peers.len == GossipSubD
        result = true

      check:
        waitFor(testRun()) == true

    test "`getGossipPeers` - should not crash on missing topics in gossip":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.mesh[topic] = initHashSet[string]()
        gossipSub.gossipsub[topic] = initHashSet[string]()
        for i in 0..<30:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          if i mod 2 == 0:
            gossipSub.mesh[topic].incl(peerInfo.id)
          else:
            gossipSub.gossipsub[topic].incl(peerInfo.id)

        let peers = gossipSub.getGossipPeers()
        check peers.len == GossipSubD
        result = true

      check:
        waitFor(testRun()) == true

    test "`getGossipPeers` - should not crash on missing topics in gossip":
      proc testRun(): Future[bool] {.async.} =
        let gossipSub = newPubSub(TestGossipSub,
                                  PeerInfo.init(PrivateKey.random(RSA)))

        proc handler(peer: PubSubPeer, msg: seq[RPCMsg]) {.async.} =
          discard

        proc writeHandler(data: seq[byte]) {.async.} =
          discard

        let topic = "foobar"
        gossipSub.mesh[topic] = initHashSet[string]()
        gossipSub.fanout[topic] = initHashSet[string]()
        for i in 0..<30:
          let conn = newConnection(newBufferStream(writeHandler))
          let peerInfo = PeerInfo.init(PrivateKey.random(RSA))
          conn.peerInfo = peerInfo
          gossipSub.peers[peerInfo.id] = newPubSubPeer(peerInfo, GossipSubCodec)
          gossipSub.peers[peerInfo.id].handler = handler
          if i mod 2 == 0:
            gossipSub.mesh[topic].incl(peerInfo.id)
          else:
            gossipSub.fanout[topic].incl(peerInfo.id)

        let peers = gossipSub.getGossipPeers()
        check peers.len == 0
        result = true

      check:
        waitFor(testRun()) == true
