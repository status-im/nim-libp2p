include ../../libp2p/protocols/pubsub/gossipsub

{.used.}

import options
import bearssl
import stew/byteutils
import ../../libp2p/builders
import ../../libp2p/errors
import ../../libp2p/crypto/crypto
import ../../libp2p/stream/bufferstream
import ../../libp2p/switch

import ../helpers

type
  TestGossipSub = ref object of GossipSub

proc noop(data: seq[byte]) {.async, gcsafe.} = discard

proc getPubSubPeer(p: TestGossipSub, peerId: PeerID): PubSubPeer
  {.raises: [Defect, DialFailedError].} =
  proc getConn(): Future[Connection] {.raises: [Defect, DialFailedError].} =
    p.switch.dial(peerId, GossipSubCodec)

  proc dropConn(peer: PubSubPeer) =
    discard # we don't care about it here yet

  let pubSubPeer = newPubSubPeer(peerId, getConn, dropConn, nil, GossipSubCodec)
  debug "created new pubsub peer", peerId

  p.peers[peerId] = pubSubPeer

  onNewPeer(p, pubSubPeer)
  pubSubPeer

proc randomPeerInfo(): PeerInfo
  {.raises: [Defect, ResultError[cstring]].} =
  PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())

suite "GossipSub internal":
  teardown:
    checkTrackers()

  asyncTest "subscribe/unsubscribeAll":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(topic: string, data: seq[byte]): Future[void] {.gcsafe.} =
      discard

    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.sendConn = conn
      gossipSub.gossipsub[topic].incl(peer)

    # test via dynamic dispatch
    gossipSub.PubSub.subscribe(topic, handler)

    check:
      gossipSub.topics.contains(topic)
      gossipSub.gossipsub[topic].len() > 0
      gossipSub.mesh[topic].len() > 0

    # test via dynamic dispatch
    gossipSub.PubSub.unsubscribeAll(topic)

    check:
      topic notin gossipSub.topics # not in local topics
      topic notin gossipSub.mesh # not in mesh
      topic in gossipSub.gossipsub # but still in gossipsub table (for fanning out)

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "topic params":
    let params = TopicParams.init()
    params.validateParameters().tryGet()

  asyncTest "`rebalanceMesh` Degree Lo":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.sendConn = conn
      gossipSub.gossipsub[topic].incl(peer)

    check gossipSub.peers.len == 15
    gossipSub.rebalanceMesh(topic)
    check gossipSub.mesh[topic].len == gossipSub.parameters.d

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "rebalanceMesh - bad peers":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    var scoreLow = -11'f64
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.sendConn = conn
      peer.score = scoreLow
      gossipSub.gossipsub[topic].incl(peer)
      scoreLow += 1.0

    check gossipSub.peers.len == 15
    gossipSub.rebalanceMesh(topic)
    # low score peers should not be in mesh, that's why the count must be 4
    check gossipSub.mesh[topic].len == 4
    for peer in gossipSub.mesh[topic]:
      check peer.score >= 0.0

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`rebalanceMesh` Degree Hi":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      gossipSub.grafted(peer, topic)
      gossipSub.mesh[topic].incl(peer)

    check gossipSub.mesh[topic].len == 15
    gossipSub.rebalanceMesh(topic)
    check gossipSub.mesh[topic].len == gossipSub.parameters.d + gossipSub.parameters.dScore

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`replenishFanout` Degree Lo":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      var peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      gossipSub.gossipsub[topic].incl(peer)

    check gossipSub.gossipsub[topic].len == 15
    gossipSub.replenishFanout(topic)
    check gossipSub.fanout[topic].len == gossipSub.parameters.d

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`dropFanoutPeers` drop expired fanout topics":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.fanout[topic] = initHashSet[PubSubPeer]()
    gossipSub.lastFanoutPubSub[topic] = Moment.fromNow(1.millis)
    await sleepAsync(5.millis) # allow the topic to expire

    var conns = newSeq[Connection]()
    for i in 0..<6:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      gossipSub.fanout[topic].incl(peer)

    check gossipSub.fanout[topic].len == gossipSub.parameters.d

    gossipSub.dropFanoutPeers()
    check topic notin gossipSub.fanout

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`dropFanoutPeers` leave unexpired fanout topics":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic1 = "foobar1"
    let topic2 = "foobar2"
    gossipSub.topicParams[topic1] = TopicParams.init()
    gossipSub.topicParams[topic2] = TopicParams.init()
    gossipSub.fanout[topic1] = initHashSet[PubSubPeer]()
    gossipSub.fanout[topic2] = initHashSet[PubSubPeer]()
    gossipSub.lastFanoutPubSub[topic1] = Moment.fromNow(1.millis)
    gossipSub.lastFanoutPubSub[topic2] = Moment.fromNow(1.minutes)
    await sleepAsync(5.millis) # allow the topic to expire

    var conns = newSeq[Connection]()
    for i in 0..<6:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      gossipSub.fanout[topic1].incl(peer)
      gossipSub.fanout[topic2].incl(peer)

    check gossipSub.fanout[topic1].len == gossipSub.parameters.d
    check gossipSub.fanout[topic2].len == gossipSub.parameters.d

    gossipSub.dropFanoutPeers()
    check topic1 notin gossipSub.fanout
    check topic2 in gossipSub.fanout

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`getGossipPeers` - should gather up to degree D non intersecting peers":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.fanout[topic] = initHashSet[PubSubPeer]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    var conns = newSeq[Connection]()

    # generate mesh and fanout peers
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      if i mod 2 == 0:
        gossipSub.fanout[topic].incl(peer)
      else:
        gossipSub.grafted(peer, topic)
        gossipSub.mesh[topic].incl(peer)

    # generate gossipsub (free standing) peers
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      gossipSub.gossipsub[topic].incl(peer)

    # generate messages
    var seqno = 0'u64
    for i in 0..5:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      inc seqno
      let msg = Message.init(some(peerInfo), ("HELLO" & $i).toBytes(), topic, some(seqno), false)
      gossipSub.mcache.put(gossipSub.msgIdProvider(msg), msg)

    check gossipSub.fanout[topic].len == 15
    check gossipSub.mesh[topic].len == 15
    check gossipSub.gossipsub[topic].len == 15

    let peers = gossipSub.getGossipPeers()
    check peers.len == gossipSub.parameters.d
    for p in peers.keys:
      check not gossipSub.fanout.hasPeerID(topic, p.peerId)
      check not gossipSub.mesh.hasPeerID(topic, p.peerId)

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`getGossipPeers` - should not crash on missing topics in mesh":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.fanout[topic] = initHashSet[PubSubPeer]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    var conns = newSeq[Connection]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      if i mod 2 == 0:
        gossipSub.fanout[topic].incl(peer)
      else:
        gossipSub.gossipsub[topic].incl(peer)

    # generate messages
    var seqno = 0'u64
    for i in 0..5:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      inc seqno
      let msg = Message.init(some(peerInfo), ("HELLO" & $i).toBytes(), topic, some(seqno), false)
      gossipSub.mcache.put(gossipSub.msgIdProvider(msg), msg)

    let peers = gossipSub.getGossipPeers()
    check peers.len == gossipSub.parameters.d

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`getGossipPeers` - should not crash on missing topics in fanout":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    var conns = newSeq[Connection]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      if i mod 2 == 0:
        gossipSub.mesh[topic].incl(peer)
        gossipSub.grafted(peer, topic)
      else:
        gossipSub.gossipsub[topic].incl(peer)

    # generate messages
    var seqno = 0'u64
    for i in 0..5:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      inc seqno
      let msg = Message.init(some(peerInfo), ("HELLO" & $i).toBytes(), topic, some(seqno), false)
      gossipSub.mcache.put(gossipSub.msgIdProvider(msg), msg)

    let peers = gossipSub.getGossipPeers()
    check peers.len == gossipSub.parameters.d

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "`getGossipPeers` - should not crash on missing topics in gossip":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      discard

    let topic = "foobar"
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.fanout[topic] = initHashSet[PubSubPeer]()
    var conns = newSeq[Connection]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      if i mod 2 == 0:
        gossipSub.mesh[topic].incl(peer)
        gossipSub.grafted(peer, topic)
      else:
        gossipSub.fanout[topic].incl(peer)

    # generate messages
    var seqno = 0'u64
    for i in 0..5:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      inc seqno
      let msg = Message.init(some(peerInfo), ("bar" & $i).toBytes(), topic, some(seqno), false)
      gossipSub.mcache.put(gossipSub.msgIdProvider(msg), msg)

    let peers = gossipSub.getGossipPeers()
    check peers.len == 0

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "Drop messages of topics without subscription":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      check false

    let topic = "foobar"
    var conns = newSeq[Connection]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler

    # generate messages
    var seqno = 0'u64
    for i in 0..5:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      inc seqno
      let msg = Message.init(some(peerInfo), ("bar" & $i).toBytes(), topic, some(seqno), false)
      await gossipSub.rpcHandler(peer, RPCMsg(messages: @[msg]))

    check gossipSub.mcache.msgs.len == 0

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "Disconnect bad peers":
    let gossipSub = TestGossipSub.init(newStandardSwitch())
    gossipSub.parameters.disconnectBadPeers = true
    gossipSub.parameters.appSpecificWeight = 1.0
    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      check false

    let topic = "foobar"
    var conns = newSeq[Connection]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.sendConn = conn
      peer.handler = handler
      peer.appScore = gossipSub.parameters.graylistThreshold - 1
      gossipSub.gossipsub.mgetOrPut(topic, initHashSet[PubSubPeer]()).incl(peer)
      gossipSub.switch.connManager.storeConn(conn)

    gossipSub.updateScores()

    await sleepAsync(100.millis)

    check:
      # test our disconnect mechanics
      gossipSub.gossipsub.peers(topic) == 0
      # also ensure we cleanup properly the peersInIP table
      gossipSub.peersInIP.len == 0

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "subscription limits":
    let gossipSub = TestGossipSub.init(newStandardSwitch())
    gossipSub.topicsHigh = 10

    var tooManyTopics: seq[string]
    for i in 0..gossipSub.topicsHigh + 10:
      tooManyTopics &= "topic" & $i
    let lotOfSubs = RPCMsg.withSubs(tooManyTopics, true)

    let conn = newBufferStream(noop)
    let peerInfo = randomPeerInfo()
    conn.peerInfo = peerInfo
    let peer = gossipSub.getPubSubPeer(peerInfo.peerId)

    await gossipSub.rpcHandler(peer, lotOfSubs)

    check:
      gossipSub.gossipSub.len == gossipSub.topicsHigh
      peer.behaviourPenalty > 0.0

    await conn.close()
    await gossipSub.switch.stop()

  asyncTest "rebalanceMesh fail due to backoff":
    let gossipSub = TestGossipSub.init(newStandardSwitch())
    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    for i in 0..<15:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.sendConn = conn
      gossipSub.gossipsub[topic].incl(peer)
      gossipSub.backingOff
        .mgetOrPut(topic, initTable[PeerID, Moment]())
        .add(peerInfo.peerId, Moment.now() + 1.hours)
      let prunes = gossipSub.handleGraft(peer, @[ControlGraft(topicID: topic)])
      # there must be a control prune due to violation of backoff
      check prunes.len != 0

    check gossipSub.peers.len == 15
    gossipSub.rebalanceMesh(topic)
    # expect 0 since they are all backing off
    check gossipSub.mesh[topic].len == 0

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "rebalanceMesh Degree Hi - audit scenario":
    let gossipSub = TestGossipSub.init(newStandardSwitch())
    let topic = "foobar"
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    gossipSub.topicParams[topic] = TopicParams.init()
    gossipSub.parameters.dScore = 4
    gossipSub.parameters.d = 6
    gossipSub.parameters.dOut = 3
    gossipSub.parameters.dHigh = 12
    gossipSub.parameters.dLow = 4

    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    for i in 0..<6:
      let conn = newBufferStream(noop)
      conn.transportDir = Direction.In
      conns &= conn
      let peerInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.score = 40.0
      peer.sendConn = conn
      gossipSub.grafted(peer, topic)
      gossipSub.mesh[topic].incl(peer)

    for i in 0..<7:
      let conn = newBufferStream(noop)
      conn.transportDir = Direction.Out
      conns &= conn
      let peerInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.score = 10.0
      peer.sendConn = conn
      gossipSub.grafted(peer, topic)
      gossipSub.mesh[topic].incl(peer)

    check gossipSub.mesh[topic].len == 13
    gossipSub.rebalanceMesh(topic)
    # ensure we are above dlow
    check gossipSub.mesh[topic].len > gossipSub.parameters.dLow
    var outbound = 0
    for peer in gossipSub.mesh[topic]:
      if peer.sendConn.transportDir == Direction.Out:
        inc outbound
    # ensure we give priority and keep at least dOut outbound peers
    check outbound >= gossipSub.parameters.dOut

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()

  asyncTest "handleIHave/Iwant tests":
    let gossipSub = TestGossipSub.init(newStandardSwitch())

    proc handler(peer: PubSubPeer, msg: RPCMsg) {.async.} =
      check false

    let topic = "foobar"
    var conns = newSeq[Connection]()
    gossipSub.gossipsub[topic] = initHashSet[PubSubPeer]()
    gossipSub.mesh[topic] = initHashSet[PubSubPeer]()
    for i in 0..<30:
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      peer.handler = handler
      gossipSub.grafted(peer, topic)
      gossipSub.mesh[topic].incl(peer)

    block:
      # should ignore no budget peer
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      let id = @[0'u8, 1, 2, 3]
      let msg = ControlIHave(
        topicID: topic,
        messageIDs: @[id, id, id]
      )
      peer.iHaveBudget = 0
      let iwants = gossipSub.handleIHave(peer, @[msg])
      check: iwants.messageIDs.len == 0

    block:
      # given duplicate ihave should generate only one iwant
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      let id = @[0'u8, 1, 2, 3]
      let msg = ControlIHave(
        topicID: topic,
        messageIDs: @[id, id, id]
      )
      let iwants = gossipSub.handleIHave(peer, @[msg])
      check: iwants.messageIDs.len == 1

    block:
      # given duplicate iwant should generate only one message
      let conn = newBufferStream(noop)
      conns &= conn
      let peerInfo = randomPeerInfo()
      conn.peerInfo = peerInfo
      let peer = gossipSub.getPubSubPeer(peerInfo.peerId)
      let id = @[0'u8, 1, 2, 3]
      gossipSub.mcache.put(id, Message())
      let msg = ControlIWant(
        messageIDs: @[id, id, id]
      )
      let genmsg = gossipSub.handleIWant(peer, @[msg])
      check: genmsg.len == 1

    check gossipSub.mcache.msgs.len == 1

    await allFuturesThrowing(conns.mapIt(it.close()))
    await gossipSub.switch.stop()
