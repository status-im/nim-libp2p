# compile time options here
const
  libp2p_pubsub_sign {.booldefine.} = true
  libp2p_pubsub_verify {.booldefine.} = true
  libp2p_pubsub_anonymize {.booldefine.} = false

import random, tables
import chronos
import ../../libp2p/[builders,
                     protocols/pubsub/pubsub,
                     protocols/pubsub/gossipsub,
                     protocols/pubsub/floodsub,
                     protocols/secure/secure]

export builders

randomize()

proc generateNodes*(
  num: Natural,
  secureManagers: openarray[SecureProtocol] = [
    SecureProtocol.Noise
  ],
  msgIdProvider: MsgIdProvider = nil,
  gossip: bool = false,
  triggerSelf: bool = false,
  verifySignature: bool = libp2p_pubsub_verify,
  anonymize: bool = libp2p_pubsub_anonymize,
  sign: bool = libp2p_pubsub_sign): seq[PubSub] =

  for i in 0..<num:
    let switch = newStandardSwitch(secureManagers = secureManagers)
    let pubsub = if gossip:
      let g = GossipSub.init(
        switch = switch,
        triggerSelf = triggerSelf,
        verifySignature = verifySignature,
        sign = sign,
        msgIdProvider = msgIdProvider,
        anonymize = anonymize,
        parameters = (var p = GossipSubParams.init(); p.floodPublish = false; p))
      # set some testing params, to enable scores
      g.topicParams.mgetOrPut("foobar", TopicParams.init()).topicWeight = 1.0
      g.topicParams.mgetOrPut("foo", TopicParams.init()).topicWeight = 1.0
      g.topicParams.mgetOrPut("bar", TopicParams.init()).topicWeight = 1.0
      g.PubSub
    else:
      FloodSub.init(
        switch = switch,
        triggerSelf = triggerSelf,
        verifySignature = verifySignature,
        sign = sign,
        msgIdProvider = msgIdProvider,
        anonymize = anonymize).PubSub

    switch.mount(pubsub)
    result.add(pubsub)

proc subscribeNodes*(nodes: seq[PubSub]) {.async.} =
  for dialer in nodes:
    for node in nodes:
      if dialer.switch.peerInfo.peerId != node.switch.peerInfo.peerId:
        await dialer.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)

proc subscribeSparseNodes*(nodes: seq[PubSub], degree: int = 2) {.async.} =
  if nodes.len < degree:
    raise (ref CatchableError)(msg: "nodes count needs to be greater or equal to degree!")

  for i, dialer in nodes:
    if (i mod degree) != 0:
      continue

    for node in nodes:
      if dialer.switch.peerInfo.peerId != node.peerInfo.peerId:
        await dialer.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)

proc subscribeRandom*(nodes: seq[PubSub]) {.async.} =
  for dialer in nodes:
    var dialed: seq[PeerID]
    while dialed.len < nodes.len - 1:
      let node = sample(nodes)
      if node.peerInfo.peerId notin dialed:
        if dialer.peerInfo.peerId != node.peerInfo.peerId:
          await dialer.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)
          dialed.add(node.peerInfo.peerId)
