## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[tables, sets, options, sequtils, random]
import chronos, chronicles, metrics, bearssl
import ./pubsub,
       ./floodsub,
       ./pubsubpeer,
       ./peertable,
       ./mcache,
       ./timedcache,
       ./rpc/[messages, message],
       ../protocol,
       ../../stream/connection,
       ../../peerinfo,
       ../../peerid,
       ../../utility,
       ../../switch

import stew/results
export results

import gossipsub/[types, scoring, behavior]
export types
export scoring
export behavior

logScope:
  topics = "libp2p gossipsub"

declareCounter(libp2p_gossipsub_failed_publish, "number of failed publish")
declareCounter(libp2p_gossipsub_invalid_topic_subscription, "number of invalid topic subscriptions that happened")

proc init*(_: type[GossipSubParams]): GossipSubParams =
  GossipSubParams(
      explicit: true,
      pruneBackoff: 1.minutes,
      floodPublish: true,
      gossipFactor: 0.25,
      d: GossipSubD,
      dLow: GossipSubDlo,
      dHigh: GossipSubDhi,
      dScore: GossipSubDlo,
      dOut: GossipSubDlo - 1, # DLow - 1
      dLazy: GossipSubD, # Like D
      heartbeatInterval: GossipSubHeartbeatInterval,
      historyLength: GossipSubHistoryLength,
      historyGossip: GossipSubHistoryGossip,
      fanoutTTL: GossipSubFanoutTTL,
      seenTTL: 2.minutes,
      gossipThreshold: -100,
      publishThreshold: -1000,
      graylistThreshold: -10000,
      opportunisticGraftThreshold: 0,
      decayInterval: 1.seconds,
      decayToZero: 0.01,
      retainScore: 2.minutes,
      appSpecificWeight: 0.0,
      ipColocationFactorWeight: 0.0,
      ipColocationFactorThreshold: 1.0,
      behaviourPenaltyWeight: -1.0,
      behaviourPenaltyDecay: 0.999,
      disconnectBadPeers: false
    )

proc validateParameters*(parameters: GossipSubParams): Result[void, cstring] =
  if  (parameters.dOut >= parameters.dLow) or
      (parameters.dOut > (parameters.d div 2)):
    err("gossipsub: dOut parameter error, " &
      "Number of outbound connections to keep in the mesh. " &
      "Must be less than D_lo and at most D/2")
  elif parameters.gossipThreshold >= 0:
    err("gossipsub: gossipThreshold parameter error, Must be < 0")
  elif parameters.publishThreshold >= parameters.gossipThreshold:
    err("gossipsub: publishThreshold parameter error, Must be < gossipThreshold")
  elif parameters.graylistThreshold >= parameters.publishThreshold:
    err("gossipsub: graylistThreshold parameter error, Must be < publishThreshold")
  elif parameters.acceptPXThreshold < 0:
    err("gossipsub: acceptPXThreshold parameter error, Must be >= 0")
  elif parameters.opportunisticGraftThreshold < 0:
    err("gossipsub: opportunisticGraftThreshold parameter error, Must be >= 0")
  elif parameters.decayToZero > 0.5 or parameters.decayToZero <= 0.0:
    err("gossipsub: decayToZero parameter error, Should be close to 0.0")
  elif parameters.appSpecificWeight < 0:
    err("gossipsub: appSpecificWeight parameter error, Must be positive")
  elif parameters.ipColocationFactorWeight > 0:
    err("gossipsub: ipColocationFactorWeight parameter error, Must be negative or 0")
  elif parameters.ipColocationFactorThreshold < 1.0:
    err("gossipsub: ipColocationFactorThreshold parameter error, Must be at least 1")
  elif parameters.behaviourPenaltyWeight >= 0:
    err("gossipsub: behaviourPenaltyWeight parameter error, Must be negative")
  elif parameters.behaviourPenaltyDecay < 0 or parameters.behaviourPenaltyDecay >= 1:
    err("gossipsub: behaviourPenaltyDecay parameter error, Must be between 0 and 1")
  else:
    ok()

proc init*(_: type[TopicParams]): TopicParams =
  TopicParams(
    topicWeight: 0.0, # disabled by default
    timeInMeshWeight: 0.01,
    timeInMeshQuantum: 1.seconds,
    timeInMeshCap: 10.0,
    firstMessageDeliveriesWeight: 1.0,
    firstMessageDeliveriesDecay: 0.5,
    firstMessageDeliveriesCap: 10.0,
    meshMessageDeliveriesWeight: -1.0,
    meshMessageDeliveriesDecay: 0.5,
    meshMessageDeliveriesCap: 10,
    meshMessageDeliveriesThreshold: 1,
    meshMessageDeliveriesWindow: 5.milliseconds,
    meshMessageDeliveriesActivation: 10.seconds,
    meshFailurePenaltyWeight: -1.0,
    meshFailurePenaltyDecay: 0.5,
    invalidMessageDeliveriesWeight: -1.0,
    invalidMessageDeliveriesDecay: 0.5
  )

proc validateParameters*(parameters: TopicParams): Result[void, cstring] =
  if parameters.timeInMeshWeight <= 0.0 or parameters.timeInMeshWeight > 1.0:
    err("gossipsub: timeInMeshWeight parameter error, Must be a small positive value")
  elif parameters.timeInMeshCap <= 0.0:
    err("gossipsub: timeInMeshCap parameter error, Should be a positive value")
  elif parameters.firstMessageDeliveriesWeight <= 0.0:
    err("gossipsub: firstMessageDeliveriesWeight parameter error, Should be a positive value")
  elif parameters.meshMessageDeliveriesWeight >= 0.0:
    err("gossipsub: meshMessageDeliveriesWeight parameter error, Should be a negative value")
  elif parameters.meshMessageDeliveriesThreshold <= 0.0:
    err("gossipsub: meshMessageDeliveriesThreshold parameter error, Should be a positive value")
  elif parameters.meshMessageDeliveriesCap < parameters.meshMessageDeliveriesThreshold:
    err("gossipsub: meshMessageDeliveriesCap parameter error, Should be >= meshMessageDeliveriesThreshold")
  elif parameters.meshFailurePenaltyWeight >= 0.0:
    err("gossipsub: meshFailurePenaltyWeight parameter error, Should be a negative value")
  elif parameters.invalidMessageDeliveriesWeight >= 0.0:
    err("gossipsub: invalidMessageDeliveriesWeight parameter error, Should be a negative value")
  else:
    ok()

method init*(g: GossipSub) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/floodsub/1.0.0``, etc...
    ##
    try:
      await g.handleConn(conn, proto)
    except CancelledError:
      # This is top-level procedure which will work as separate task, so it
      # do not need to propogate CancelledError.
      trace "Unexpected cancellation in gossipsub handler", conn
    except CatchableError as exc:
      trace "GossipSub handler leaks an error", exc = exc.msg, conn

  g.handler = handler
  g.codecs &= GossipSubCodec
  g.codecs &= GossipSubCodec_10

method onNewPeer(g: GossipSub, peer: PubSubPeer) =
  g.withPeerStats(peer.peerId) do (stats: var PeerStats):
    # Make sure stats and peer information match, even when reloading peer stats
    # from a previous connection
    peer.score = stats.score
    peer.appScore = stats.appScore
    peer.behaviourPenalty = stats.behaviourPenalty

    peer.iWantBudget = IWantPeerBudget
    peer.iHaveBudget = IHavePeerBudget

method onPubSubPeerEvent*(p: GossipSub, peer: PubsubPeer, event: PubSubPeerEvent) {.gcsafe.} =
  case event.kind
  of PubSubPeerEventKind.Connected:
    discard
  of PubSubPeerEventKind.Disconnected:
    # If a send connection is lost, it's better to remove peer from the mesh -
    # if it gets reestablished, the peer will be readded to the mesh, and if it
    # doesn't, well.. then we hope the peer is going away!
    for topic, peers in p.mesh.mpairs():
      p.pruned(peer, topic)
      peers.excl(peer)
    for _, peers in p.fanout.mpairs():
      peers.excl(peer)

  procCall FloodSub(p).onPubSubPeerEvent(peer, event)

method unsubscribePeer*(g: GossipSub, peer: PeerID) =
  ## handle peer disconnects
  ##

  trace "unsubscribing gossipsub peer", peer
  let pubSubPeer = g.peers.getOrDefault(peer)
  if pubSubPeer.isNil:
    trace "no peer to unsubscribe", peer
    return

  # remove from peer IPs collection too
  if pubSubPeer.address.isSome():
    g.peersInIP.withValue(pubSubPeer.address.get(), s):
      s[].excl(pubSubPeer.peerId)
      if s[].len == 0:
        g.peersInIP.del(pubSubPeer.address.get())

  for t in toSeq(g.gossipsub.keys):
    g.gossipsub.removePeer(t, pubSubPeer)
    # also try to remove from explicit table here
    g.explicit.removePeer(t, pubSubPeer)

  for t in toSeq(g.mesh.keys):
    trace "pruning unsubscribing peer", pubSubPeer, score = pubSubPeer.score
    g.pruned(pubSubPeer, t)
    g.mesh.removePeer(t, pubSubPeer)

  for t in toSeq(g.fanout.keys):
    g.fanout.removePeer(t, pubSubPeer)

  g.peerStats.withValue(peer, stats):
    for topic, info in stats[].topicInfos.mpairs:
      info.firstMessageDeliveries = 0

  procCall FloodSub(g).unsubscribePeer(peer)

method subscribeTopic*(g: GossipSub,
                       topic: string,
                       subscribe: bool,
                       peer: PubSubPeer) {.gcsafe.} =
  logScope:
    peer
    topic

  # this is a workaround for a race condition
  # that can happen if we disconnect the peer very early
  # in the future we might use this as a test case
  # and eventually remove this workaround
  if subscribe and peer.peerId notin g.peers:
    trace "ignoring unknown peer"
    return

  if subscribe and not(isNil(g.subscriptionValidator)) and not(g.subscriptionValidator(topic)):
    # this is a violation, so warn should be in order
    trace "ignoring invalid topic subscription", topic, peer
    libp2p_gossipsub_invalid_topic_subscription.inc()
    return

  if subscribe:
    trace "peer subscribed to topic"

    # subscribe remote peer to the topic
    discard g.gossipsub.addPeer(topic, peer)
    if peer.peerId in g.parameters.directPeers:
      discard g.explicit.addPeer(topic, peer)
  else:
    trace "peer unsubscribed from topic"

    # unsubscribe remote peer from the topic
    g.gossipsub.removePeer(topic, peer)
    g.mesh.removePeer(topic, peer)
    g.fanout.removePeer(topic, peer)
    if peer.peerId in g.parameters.directPeers:
      g.explicit.removePeer(topic, peer)

  trace "gossip peers", peers = g.gossipsub.peers(topic), topic

method rpcHandler*(g: GossipSub,
                  peer: PubSubPeer,
                  rpcMsg: RPCMsg) {.async.} =
  # base will check the amount of subscriptions and process subscriptions
  # also will update some metrics
  await procCall PubSub(g).rpcHandler(peer, rpcMsg)

  # the above call applied limtis to subs number
  # in gossipsub we want to apply scoring as well
  if rpcMsg.subscriptions.len > g.topicsHigh:
    debug "received an rpc message with an oversized amount of subscriptions",  peer,
                                                                                size = rpcMsg.subscriptions.len,
                                                                                limit = g.topicsHigh
    peer.behaviourPenalty += 0.1

  for msg in rpcMsg.messages:                         # for every message
    let msgId = g.msgIdProvider(msg)

    # avoid the remote peer from controlling the seen table hashing
    # by adding random bytes to the ID we ensure we randomize the IDs
    # we do only for seen as this is the great filter from the external world
    if g.seen.put(msgId & g.randomBytes):
      trace "Dropping already-seen message", msgId = shortLog(msgId), peer

      # make sure to update score tho before continuing
      for t in msg.topicIDs:
        if t notin g.topics:
          continue
                         # for every topic in the message
        let topicParams = g.topicParams.mgetOrPut(t, TopicParams.init())
                                                # if in mesh add more delivery score
        g.withPeerStats(peer.peerId) do (stats: var PeerStats):
          stats.topicInfos.withValue(t, tstats):
            if tstats[].inMesh:
              # TODO: take into account meshMessageDeliveriesWindow
              # score only if messages are not too old.
              tstats[].meshMessageDeliveries += 1
              if tstats[].meshMessageDeliveries > topicParams.meshMessageDeliveriesCap:
                tstats[].meshMessageDeliveries = topicParams.meshMessageDeliveriesCap
          do: # make sure we don't loose this information
            stats.topicInfos[t] = TopicInfo(meshMessageDeliveries: 1)

      # onto the next message
      continue

    # avoid processing messages we are not interested in
    if msg.topicIDs.allIt(it notin g.topics):
      debug "Dropping message of topic without subscription", msgId = shortLog(msgId), peer
      continue

    if (msg.signature.len > 0 or g.verifySignature) and not msg.verify():
      # always validate if signature is present or required
      debug "Dropping message due to failed signature verification",
        msgId = shortLog(msgId), peer
      g.punishInvalidMessage(peer, msg.topicIDs)
      continue

    if msg.seqno.len > 0 and msg.seqno.len != 8:
      # if we have seqno should be 8 bytes long
      debug "Dropping message due to invalid seqno length",
        msgId = shortLog(msgId), peer
      g.punishInvalidMessage(peer, msg.topicIDs)
      continue

    # g.anonymize needs no evaluation when receiving messages
    # as we have a "lax" policy and allow signed messages

    let validation = await g.validate(msg)
    case validation
    of ValidationResult.Reject:
      debug "Dropping message after validation, reason: reject",
        msgId = shortLog(msgId), peer
      g.punishInvalidMessage(peer, msg.topicIDs)
      continue
    of ValidationResult.Ignore:
      debug "Dropping message after validation, reason: ignore",
        msgId = shortLog(msgId), peer
      continue
    of ValidationResult.Accept:
      discard

    # store in cache only after validation
    g.mcache.put(msgId, msg)

    var toSendPeers = initHashSet[PubSubPeer]()
    for t in msg.topicIDs:                      # for every topic in the message
      if t notin g.topics:
        continue

      let topicParams = g.topicParams.mgetOrPut(t, TopicParams.init())

      g.withPeerStats(peer.peerId) do(stats: var PeerStats):
        stats.topicInfos.withValue(t, tstats):
                                                    # contribute to peer score first delivery
          tstats[].firstMessageDeliveries += 1
          if tstats[].firstMessageDeliveries > topicParams.firstMessageDeliveriesCap:
            tstats[].firstMessageDeliveries = topicParams.firstMessageDeliveriesCap

                                                    # if in mesh add more delivery score
          if tstats[].inMesh:
            tstats[].meshMessageDeliveries += 1
            if tstats[].meshMessageDeliveries > topicParams.meshMessageDeliveriesCap:
              tstats[].meshMessageDeliveries = topicParams.meshMessageDeliveriesCap
        do: # make sure we don't loose this information
          stats.topicInfos[t] = TopicInfo(firstMessageDeliveries: 1, meshMessageDeliveries: 1)

      g.floodsub.withValue(t, peers): toSendPeers.incl(peers[])
      g.mesh.withValue(t, peers): toSendPeers.incl(peers[])

      await handleData(g, t, msg.data)

    # In theory, if topics are the same in all messages, we could batch - we'd
    # also have to be careful to only include validated messages
    let sendingTo = toSeq(toSendPeers)
    g.broadcast(sendingTo, RPCMsg(messages: @[msg]))
    trace "forwared message to peers", peers = sendingTo.len, msgId, peer
    for topic in msg.topicIDs:
      if g.knownTopics.contains(topic):
        libp2p_pubsub_messages_rebroadcasted.inc(sendingTo.len.int64, labelValues = [topic])
      else:
        libp2p_pubsub_messages_rebroadcasted.inc(sendingTo.len.int64, labelValues = ["generic"])

  if rpcMsg.control.isSome:
    let control = rpcMsg.control.get()
    g.handlePrune(peer, control.prune)

    var respControl: ControlMessage
    respControl.iwant.add(g.handleIHave(peer, control.ihave))
    respControl.prune.add(g.handleGraft(peer, control.graft))
    let messages = g.handleIWant(peer, control.iwant)

    if respControl.graft.len > 0 or respControl.prune.len > 0 or
      respControl.ihave.len > 0 or messages.len > 0:
      # iwant and prunes from here, also messages

      for smsg in messages:
        for topic in smsg.topicIDs:
          if g.knownTopics.contains(topic):
            libp2p_pubsub_broadcast_messages.inc(labelValues = [topic])
          else:
            libp2p_pubsub_broadcast_messages.inc(labelValues = ["generic"])
      libp2p_pubsub_broadcast_iwant.inc(respControl.iwant.len.int64)
      for prune in respControl.prune:
        if g.knownTopics.contains(prune.topicID):
          libp2p_pubsub_broadcast_prune.inc(labelValues = [prune.topicID])
        else:
          libp2p_pubsub_broadcast_prune.inc(labelValues = ["generic"])
      trace "sending control message", msg = shortLog(respControl), peer
      g.send(
        peer,
        RPCMsg(control: some(respControl), messages: messages))

method subscribe*(g: GossipSub,
                  topic: string,
                  handler: TopicHandler) =
  procCall PubSub(g).subscribe(topic, handler)

  # if we have a fanout on this topic break it
  if topic in g.fanout:
    g.fanout.del(topic)

  # rebalance but don't update metrics here, we do that only in the heartbeat
  g.rebalanceMesh(topic, metrics = nil)

proc unsubscribe*(g: GossipSub, topic: string) =
  var
    msg = RPCMsg.withSubs(@[topic], subscribe = false)
    gpeers = g.gossipsub.getOrDefault(topic)

  if topic in g.mesh:
    let mpeers = g.mesh.getOrDefault(topic)

    # remove mesh peers from gpeers, we send 2 different messages
    gpeers = gpeers - mpeers
    # send to peers NOT in mesh first
    g.broadcast(toSeq(gpeers), msg)

    for peer in mpeers:
      trace "pruning unsubscribeAll call peer", peer, score = peer.score
      g.pruned(peer, topic)

    g.mesh.del(topic)

    msg.control =
      some(ControlMessage(prune:
        @[ControlPrune(topicID: topic,
          peers: g.peerExchangeList(topic),
          backoff: g.parameters.pruneBackoff.seconds.uint64)]))

    # send to peers IN mesh now
    g.broadcast(toSeq(mpeers), msg)
  else:
    g.broadcast(toSeq(gpeers), msg)

  g.topicParams.del(topic)

method unsubscribeAll*(g: GossipSub, topic: string) =
  g.unsubscribe(topic)
  # finally let's remove from g.topics, do that by calling PubSub
  procCall PubSub(g).unsubscribeAll(topic)

method unsubscribe*(g: GossipSub,
                    topics: seq[TopicPair]) =
  procCall PubSub(g).unsubscribe(topics)

  for (topic, handler) in topics:
    # delete from mesh only if no handlers are left
    # (handlers are removed in pubsub unsubscribe above)
    if topic notin g.topics:
      g.unsubscribe(topic)

method publish*(g: GossipSub,
                topic: string,
                data: seq[byte]): Future[int] {.async.} =
  # base returns always 0
  discard await procCall PubSub(g).publish(topic, data)

  logScope:
    topic

  trace "Publishing message on topic", data = data.shortLog

  if topic.len <= 0: # data could be 0/empty
    debug "Empty topic, skipping publish"
    return 0

  var peers: HashSet[PubSubPeer]

  if g.parameters.floodPublish:
    # With flood publishing enabled, the mesh is used when propagating messages from other peers,
    # but a peer's own messages will always be published to all known peers in the topic.
    for peer in g.gossipsub.getOrDefault(topic):
      if peer.score >= g.parameters.publishThreshold:
        trace "publish: including flood/high score peer", peer
        peers.incl(peer)

  # add always direct peers
  peers.incl(g.explicit.getOrDefault(topic))

  if topic in g.topics: # if we're subscribed use the mesh
    peers.incl(g.mesh.getOrDefault(topic))
  else: # not subscribed, send to fanout peers
    # try optimistically
    peers.incl(g.fanout.getOrDefault(topic))
    if peers.len == 0:
      # ok we had nothing.. let's try replenish inline
      g.replenishFanout(topic)
      peers.incl(g.fanout.getOrDefault(topic))

    # even if we couldn't publish,
    # we still attempted to publish
    # on the topic, so it makes sense
    # to update the last topic publish
    # time
    g.lastFanoutPubSub[topic] = Moment.fromNow(g.parameters.fanoutTTL)

  if peers.len == 0:
    let topicPeers = g.gossipsub.getOrDefault(topic).toSeq()
    notice "No peers for topic, skipping publish",  peersOnTopic = topicPeers.len,
                                                    connectedPeers = topicPeers.filterIt(it.connected).len,
                                                    topic
    # skipping topic as our metrics finds that heavy
    libp2p_gossipsub_failed_publish.inc()
    return 0

  inc g.msgSeqno
  let
    msg =
      if g.anonymize:
        Message.init(none(PeerInfo), data, topic, none(uint64), false)
      else:
        Message.init(some(g.peerInfo), data, topic, some(g.msgSeqno), g.sign)
    msgId = g.msgIdProvider(msg)

  logScope: msgId = shortLog(msgId)

  trace "Created new message", msg = shortLog(msg), peers = peers.len

  if g.seen.put(msgId & g.randomBytes):
    # custom msgid providers might cause this
    trace "Dropping already-seen message"
    return 0

  g.mcache.put(msgId, msg)

  let peerSeq = toSeq(peers)
  g.broadcast(peerSeq, RPCMsg(messages: @[msg]))
  if g.knownTopics.contains(topic):
    libp2p_pubsub_messages_published.inc(peerSeq.len.int64, labelValues = [topic])
  else:
    libp2p_pubsub_messages_published.inc(peerSeq.len.int64, labelValues = ["generic"])

  trace "Published message to peers"

  return peers.len

proc maintainDirectPeers(g: GossipSub)
  {.async, raises: [Defect, CancelledError].} =
  while g.heartbeatRunning:
    for id, addrs in g.parameters.directPeers:
      let peer = g.peers.getOrDefault(id)
      if isNil(peer):
        trace "Attempting to dial a direct peer", peer = id
        try:
          # dial, internally connection will be stored
          let _ = await g.switch.dial(id, addrs, g.codecs)
          # populate the peer after it's connected
          discard g.getOrCreatePeer(id, g.codecs)
        except CancelledError as exc:
          trace "Direct peer dial canceled"
          raise exc
        except CatchableError as exc:
          debug "Direct peer error dialing", msg = exc.msg

    await sleepAsync(1.minutes)

method start*(g: GossipSub) {.async.} =
  trace "gossipsub start"

  if not g.heartbeatFut.isNil:
    warn "Starting gossipsub twice"
    return

  g.heartbeatRunning = true
  g.heartbeatFut = g.heartbeat()
  g.directPeersLoop = g.maintainDirectPeers()

method stop*(g: GossipSub) {.async.} =
  trace "gossipsub stop"
  if g.heartbeatFut.isNil:
    warn "Stopping gossipsub without starting it"
    return

  # stop heartbeat interval
  g.heartbeatRunning = false
  g.directPeersLoop.cancel()
  if not g.heartbeatFut.finished:
    trace "awaiting last heartbeat"
    await g.heartbeatFut
    trace "heartbeat stopped"
    g.heartbeatFut = nil

method initPubSub*(g: GossipSub)
  {.raises: [Defect, InitializationError].} =
  procCall FloodSub(g).initPubSub()

  if not g.parameters.explicit:
    g.parameters = GossipSubParams.init()

  let validationRes = g.parameters.validateParameters()
  if validationRes.isErr:
    raise newException(InitializationError, $validationRes.error)

  randomize()

  # init the floodsub stuff here, we customize timedcache in gossip!
  g.seen = TimedCache[MessageID].init(g.parameters.seenTTL)

  # init gossip stuff
  g.mcache = MCache.init(g.parameters.historyGossip, g.parameters.historyLength)
  var rng = newRng()
  g.randomBytes = newSeqUninitialized[byte](32)
  brHmacDrbgGenerate(rng[], g.randomBytes)
