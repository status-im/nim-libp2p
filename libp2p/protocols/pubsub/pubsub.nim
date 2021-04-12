## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[tables, sequtils, sets, strutils]
import chronos, chronicles, metrics
import pubsubpeer,
       rpc/[message, messages],
       ../../switch,
       ../protocol,
       ../../stream/connection,
       ../../peerid,
       ../../peerinfo,
       ../../errors,
       ../../utility

import metrics
import stew/results
export results

export PubSubPeer
export PubSubObserver
export protocol

logScope:
  topics = "libp2p pubsub"

const
  KnownLibP2PTopics* {.strdefine.} = ""
  KnownLibP2PTopicsSeq* = KnownLibP2PTopics.toLowerAscii().split(",")

declareGauge(libp2p_pubsub_peers, "pubsub peer instances")
declareGauge(libp2p_pubsub_topics, "pubsub subscribed topics")
declareCounter(libp2p_pubsub_subscriptions, "pubsub subscription operations")
declareCounter(libp2p_pubsub_unsubscriptions, "pubsub unsubscription operations")
declareGauge(libp2p_pubsub_topic_handlers, "pubsub subscribed topics handlers count", labels = ["topic"])

declareCounter(libp2p_pubsub_validation_success, "pubsub successfully validated messages")
declareCounter(libp2p_pubsub_validation_failure, "pubsub failed validated messages")
declareCounter(libp2p_pubsub_validation_ignore, "pubsub ignore validated messages")

declarePublicCounter(libp2p_pubsub_messages_published, "published messages", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_messages_rebroadcasted, "re-broadcasted messages", labels = ["topic"])

declarePublicCounter(libp2p_pubsub_broadcast_subscriptions, "pubsub broadcast subscriptions", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_broadcast_unsubscriptions, "pubsub broadcast unsubscriptions", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_broadcast_messages, "pubsub broadcast messages", labels = ["topic"])

declarePublicCounter(libp2p_pubsub_received_subscriptions, "pubsub received subscriptions", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_received_unsubscriptions, "pubsub received subscriptions", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_received_messages, "pubsub received messages", labels = ["topic"])

declarePublicCounter(libp2p_pubsub_broadcast_iwant, "pubsub broadcast iwant")

declarePublicCounter(libp2p_pubsub_broadcast_ihave, "pubsub broadcast ihave", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_broadcast_graft, "pubsub broadcast graft", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_broadcast_prune, "pubsub broadcast prune", labels = ["topic"])

declarePublicCounter(libp2p_pubsub_received_iwant, "pubsub broadcast iwant")

declarePublicCounter(libp2p_pubsub_received_ihave, "pubsub broadcast ihave", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_received_graft, "pubsub broadcast graft", labels = ["topic"])
declarePublicCounter(libp2p_pubsub_received_prune, "pubsub broadcast prune", labels = ["topic"])

type
  InitializationError* = object of LPError

  TopicHandler* = proc(topic: string,
                       data: seq[byte]): Future[void]
                       {.gcsafe, raises: [Defect].}

  ValidationResult* {.pure.} = enum
    Accept, Reject, Ignore

  ValidatorHandler* = proc(topic: string,
                           message: Message): Future[ValidationResult]
                           {.gcsafe, raises: [Defect].}

  TopicPair* = tuple[topic: string, handler: TopicHandler]

  MsgIdProvider* =
    proc(m: Message): MessageID {.noSideEffect, raises: [Defect], gcsafe.}

  SubscriptionValidator* =
    proc(topic: string): bool {.raises: [Defect], gcsafe.}

  Topic* = object
    # make this a variant type if one day we have different Params structs
    name*: string
    handler*: seq[TopicHandler]

  PubSub* = ref object of LPProtocol
    switch*: Switch                    # the switch used to dial/connect to peers
    peerInfo*: PeerInfo                # this peer's info
    topics*: Table[string, Topic]      # local topics
    peers*: Table[PeerID, PubSubPeer]  ##\
      ## Peers that we are interested to gossip with (but not necessarily
      ## yet connected to)
    triggerSelf*: bool                 # trigger own local handler on publish
    verifySignature*: bool             # enable signature verification
    sign*: bool                        # enable message signing
    validators*: Table[string, HashSet[ValidatorHandler]]
    observers: ref seq[PubSubObserver] # ref as in smart_ptr
    msgIdProvider*: MsgIdProvider      # Turn message into message id (not nil)
    msgSeqno*: uint64
    anonymize*: bool                   # if we omit fromPeer and seqno from RPC messages we send
    subscriptionValidator*: SubscriptionValidator # callback used to validate subscriptions
    topicsHigh*: int                   # the maximum number of topics we allow in a subscription message (application specific, defaults to int max)
    knownTopics*: HashSet[string]

method unsubscribePeer*(p: PubSub, peerId: PeerID) {.base.} =
  ## handle peer disconnects
  ##

  debug "unsubscribing pubsub peer", peerId
  p.peers.del(peerId)

  libp2p_pubsub_peers.set(p.peers.len.int64)

proc send*(p: PubSub, peer: PubSubPeer, msg: RPCMsg) {.raises: [Defect].} =
  ## Attempt to send `msg` to remote peer
  ##

  trace "sending pubsub message to peer", peer, msg = shortLog(msg)
  peer.send(msg, p.anonymize)

proc broadcast*(
  p: PubSub,
  sendPeers: openArray[PubSubPeer],
  msg: RPCMsg) {.raises: [Defect].} =
  ## Attempt to send `msg` to the given peers

  let npeers = sendPeers.len.int64
  for sub in msg.subscriptions:
    if sub.subscribe:
      if p.knownTopics.contains(sub.topic):
        libp2p_pubsub_broadcast_subscriptions.inc(npeers, labelValues = [sub.topic])
      else:
        libp2p_pubsub_broadcast_subscriptions.inc(npeers, labelValues = ["generic"])
    else:
      if p.knownTopics.contains(sub.topic):
        libp2p_pubsub_broadcast_unsubscriptions.inc(npeers, labelValues = [sub.topic])
      else:
        libp2p_pubsub_broadcast_unsubscriptions.inc(npeers, labelValues = ["generic"])

  for smsg in msg.messages:
    for topic in smsg.topicIDs:
      if p.knownTopics.contains(topic):
        libp2p_pubsub_broadcast_messages.inc(npeers, labelValues = [topic])
      else:
        libp2p_pubsub_broadcast_messages.inc(npeers, labelValues = ["generic"])

  if msg.control.isSome():
    libp2p_pubsub_broadcast_iwant.inc(npeers * msg.control.get().iwant.len.int64)

    let control = msg.control.get()
    for ihave in control.ihave:
      if p.knownTopics.contains(ihave.topicID):
        libp2p_pubsub_broadcast_ihave.inc(npeers, labelValues = [ihave.topicID])
      else:
        libp2p_pubsub_broadcast_ihave.inc(npeers, labelValues = ["generic"])
    for graft in control.graft:
      if p.knownTopics.contains(graft.topicID):
        libp2p_pubsub_broadcast_graft.inc(npeers, labelValues = [graft.topicID])
      else:
        libp2p_pubsub_broadcast_graft.inc(npeers, labelValues = ["generic"])
    for prune in control.prune:
      if p.knownTopics.contains(prune.topicID):
        libp2p_pubsub_broadcast_prune.inc(npeers, labelValues = [prune.topicID])
      else:
        libp2p_pubsub_broadcast_prune.inc(npeers, labelValues = ["generic"])

  trace "broadcasting messages to peers",
    peers = sendPeers.len, msg = shortLog(msg)
  for peer in sendPeers:
    p.send(peer, msg)

proc sendSubs*(p: PubSub,
               peer: PubSubPeer,
               topics: seq[string],
               subscribe: bool) =
  ## send subscriptions to remote peer
  p.send(peer, RPCMsg.withSubs(topics, subscribe))
  for topic in topics:
    if subscribe:
      if p.knownTopics.contains(topic):
        libp2p_pubsub_broadcast_subscriptions.inc(labelValues = [topic])
      else:
        libp2p_pubsub_broadcast_subscriptions.inc(labelValues = ["generic"])
    else:
      if p.knownTopics.contains(topic):
        libp2p_pubsub_broadcast_unsubscriptions.inc(labelValues = [topic])
      else:
        libp2p_pubsub_broadcast_unsubscriptions.inc(labelValues = ["generic"])

method subscribeTopic*(p: PubSub,
                       topic: string,
                       subscribe: bool,
                       peer: PubSubPeer) {.base.} =
  # both gossipsub and floodsub diverge, and this super call is not necessary right now
  # if necessary remove the assertion
  doAssert(false, "unexpected call to pubsub.subscribeTopic")

method rpcHandler*(p: PubSub,
                   peer: PubSubPeer,
                   rpcMsg: RPCMsg) {.async, base.} =
  ## handle rpc messages
  trace "processing RPC message", msg = rpcMsg.shortLog, peer
  for i in 0..<min(rpcMsg.subscriptions.len, p.topicsHigh):
    let s = rpcMsg.subscriptions[i]
    trace "about to subscribe to topic", topicId = s.topic, peer, subscribe = s.subscribe
    p.subscribeTopic(s.topic, s.subscribe, peer)

  for i in 0..<min(rpcMsg.subscriptions.len, p.topicsHigh):
    let sub = rpcMsg.subscriptions[i]
    if sub.subscribe:
      if p.knownTopics.contains(sub.topic):
        libp2p_pubsub_received_subscriptions.inc(labelValues = [sub.topic])
      else:
        libp2p_pubsub_received_subscriptions.inc(labelValues = ["generic"])
    else:
      if p.knownTopics.contains(sub.topic):
        libp2p_pubsub_received_unsubscriptions.inc(labelValues = [sub.topic])
      else:
        libp2p_pubsub_received_unsubscriptions.inc(labelValues = ["generic"])

  for smsg in rpcMsg.messages:
    for topic in smsg.topicIDs:
      if p.knownTopics.contains(topic):
        libp2p_pubsub_received_messages.inc(labelValues = [topic])
      else:
        libp2p_pubsub_received_messages.inc(labelValues = ["generic"])

  if rpcMsg.control.isSome():
    libp2p_pubsub_received_iwant.inc(rpcMsg.control.get().iwant.len.int64)

    let control = rpcMsg.control.get()
    for ihave in control.ihave:
      if p.knownTopics.contains(ihave.topicID):
        libp2p_pubsub_received_ihave.inc(labelValues = [ihave.topicID])
      else:
        libp2p_pubsub_received_ihave.inc(labelValues = ["generic"])
    for graft in control.graft:
      if p.knownTopics.contains(graft.topicID):
        libp2p_pubsub_received_graft.inc(labelValues = [graft.topicID])
      else:
        libp2p_pubsub_received_graft.inc(labelValues = ["generic"])
    for prune in control.prune:
      if p.knownTopics.contains(prune.topicID):
        libp2p_pubsub_received_prune.inc(labelValues = [prune.topicID])
      else:
        libp2p_pubsub_received_prune.inc(labelValues = ["generic"])

method onNewPeer(p: PubSub, peer: PubSubPeer) {.base.} =
  discard

method onPubSubPeerEvent*(p: PubSub, peer: PubsubPeer, event: PubsubPeerEvent) {.base, gcsafe.} =
  # Peer event is raised for the send connection in particular
  case event.kind
  of PubSubPeerEventKind.Connected:
    if p.topics.len > 0:
      p.sendSubs(peer, toSeq(p.topics.keys), true)
  of PubSubPeerEventKind.Disconnected:
    discard

proc getOrCreatePeer*(
  p: PubSub,
  peer: PeerID,
  protos: seq[string]): PubSubPeer
  {.raises: [Defect, DialFailedError].} =
  if peer notin p.peers:
    proc getConn(): Future[Connection] {.raises: [Defect, DialFailedError].} =
      p.switch.dial(peer, protos)

    proc dropConn(peer: PubSubPeer) =
      proc dropConnAsync(peer: PubsubPeer) {.async, raises: [Defect].} =
        try:
          await p.switch.disconnect(peer.peerId)
        except CatchableError as exc: # never cancelled
          trace "Failed to close connection", peer, error = exc.name, msg = exc.msg
      asyncSpawn dropConnAsync(peer)

    proc onEvent(peer: PubsubPeer, event: PubsubPeerEvent) {.gcsafe, raises: [Defect].} =
      p.onPubSubPeerEvent(peer, event)

    # create new pubsub peer
    let pubSubPeer = newPubSubPeer(peer,
                                   getConn,
                                   dropConn,
                                   onEvent,
                                   protos[0])
    debug "created new pubsub peer", peer

    p.peers[peer] = pubSubPeer
    pubSubPeer.observers = p.observers

    onNewPeer(p, pubSubPeer)

    # metrics
    libp2p_pubsub_peers.set(p.peers.len.int64)

    pubsubPeer.connect()

  return p.peers.getOrDefault(peer)

proc handleData*(p: PubSub, topic: string, data: seq[byte]): Future[void] {.async.} =
  if topic notin p.topics: return # Not subscribed

  # gather all futures without yielding to scheduler
  var futs = p.topics[topic].handler.mapIt(it(topic, data))

  try:
    futs = await allFinished(futs)
  except CancelledError:
    # propagate cancellation
    for fut in futs:
      if not(fut.finished):
        fut.cancel()

  # check for errors in futures
  for fut in futs:
    if fut.failed:
      let err = fut.readError()
      warn "Error in topic handler", msg = err.msg

method handleConn*(p: PubSub,
                   conn: Connection,
                   proto: string) {.base, async.} =
  ## handle incoming connections
  ##
  ## this proc will:
  ## 1) register a new PubSubPeer for the connection
  ## 2) register a handler with the peer;
  ##    this handler gets called on every rpc message
  ##    that the peer receives
  ## 3) ask the peer to subscribe us to every topic
  ##    that we're interested in
  ##

  if isNil(conn.peerInfo):
    trace "no valid PeerId for peer"
    await conn.close()
    return

  proc handler(peer: PubSubPeer, msg: RPCMsg): Future[void] =
    # call pubsub rpc handler
    p.rpcHandler(peer, msg)

  let peer = p.getOrCreatePeer(conn.peerInfo.peerId, @[proto])

  try:
    peer.handler = handler
    await peer.handle(conn) # spawn peer read loop
    trace "pubsub peer handler ended", conn
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "exception ocurred in pubsub handle", exc = exc.msg, conn
  finally:
    await conn.closeWithEOF()

method subscribePeer*(p: PubSub, peer: PeerID)
  {.base, raises: [Defect, DialFailedError].} =
  ## subscribe to remote peer to receive/send pubsub
  ## messages
  ##

  discard p.getOrCreatePeer(peer, p.codecs)

proc updateTopicMetrics(p: PubSub, topic: string) =
  # metrics
  libp2p_pubsub_topics.set(p.topics.len.int64)
  if p.knownTopics.contains(topic):
    libp2p_pubsub_topic_handlers.set(
      p.topics.getOrDefault(topic).handler.len.int64,
      labelValues = [topic])
  else:
    libp2p_pubsub_topic_handlers.set(0, labelValues = ["other"])
    for key, val in p.topics:
      if not p.knownTopics.contains(key):
        libp2p_pubsub_topic_handlers.inc(val.handler.len.int64, labelValues = ["other"])

method unsubscribe*(p: PubSub,
                    topics: seq[TopicPair]) {.base.} =
  ## unsubscribe from a list of ``topic`` strings
  for t in topics:
    let
      handler = t.handler
      ttopic = t.topic
    closureScope:
      p.topics.withValue(ttopic, topic):
        topic[].handler.keepIf(proc (x: auto): bool = x != handler)

        if topic[].handler.len == 0:
          # make sure we delete the topic if
          # no more handlers are left
          p.topics.del(ttopic)

          p.updateTopicMetrics(ttopic)

        libp2p_pubsub_unsubscriptions.inc()

proc unsubscribe*(p: PubSub,
                  topic: string,
                  handler: TopicHandler) =
  ## unsubscribe from a ``topic`` string
  ##
  p.unsubscribe(@[(topic, handler)])

method unsubscribeAll*(p: PubSub, topic: string) {.base.} =
  if topic notin p.topics:
    debug "unsubscribeAll called for an unknown topic", topic
  else:
    p.topics.del(topic)

    p.updateTopicMetrics(topic)

    libp2p_pubsub_unsubscriptions.inc()

method subscribe*(p: PubSub,
                  topic: string,
                  handler: TopicHandler) {.base.} =
  ## subscribe to a topic
  ##
  ## ``topic``   - a string topic to subscribe to
  ##
  ## ``handler`` - is a user provided proc
  ##               that will be triggered
  ##               on every received message
  ##
  trace "subscribing to topic", name = topic

  p.topics.mgetOrPut(topic,
    Topic(name: topic)).handler.add(handler)

  for _, peer in p.peers:
    p.sendSubs(peer, @[topic], true)

  p.updateTopicMetrics(topic)

  libp2p_pubsub_subscriptions.inc()

method publish*(p: PubSub,
                topic: string,
                data: seq[byte]): Future[int] {.base, async.} =
  ## publish to a ``topic``
  ## The return value is the number of neighbours that we attempted to send the
  ## message to, excluding self. Note that this is an optimistic number of
  ## attempts - the number of peers that actually receive the message might
  ## be lower.
  if p.triggerSelf:
    await handleData(p, topic, data)

  return 0

method initPubSub*(p: PubSub)
  {.base, raises: [Defect, InitializationError].} =
  ## perform pubsub initialization
  ##

  p.observers = new(seq[PubSubObserver])
  if p.msgIdProvider == nil:
    p.msgIdProvider = defaultMsgIdProvider

method start*(p: PubSub) {.async, base.} =
  ## start pubsub
  ##

  discard

method stop*(p: PubSub) {.async, base.} =
  ## stopt pubsub
  ##

  discard

method addValidator*(p: PubSub,
                     topic: varargs[string],
                     hook: ValidatorHandler) {.base.} =
  for t in topic:
    p.validators.mgetOrPut(t,
      initHashSet[ValidatorHandler]()).incl(hook)
    trace "adding validator for topic", topicId = t

method removeValidator*(p: PubSub,
                        topic: varargs[string],
                        hook: ValidatorHandler) {.base.} =
  for t in topic:
    p.validators.withValue(t, validators):
      validators[].excl(hook)

method validate*(p: PubSub, message: Message): Future[ValidationResult] {.async, base.} =
  var pending: seq[Future[ValidationResult]]
  trace "about to validate message"
  for topic in message.topicIDs:
    trace "looking for validators on topic", topicID = topic,
                                             registered = toSeq(p.validators.keys)
    if topic in p.validators:
      trace "running validators for topic", topicID = topic
      # TODO: add timeout to validator
      pending.add(p.validators[topic].mapIt(it(topic, message)))

  result = ValidationResult.Accept
  let futs = await allFinished(pending)
  for fut in futs:
    if fut.failed:
      result = ValidationResult.Reject
      break
    let res = fut.read()
    if res != ValidationResult.Accept:
      result = res
      break

  case result
  of ValidationResult.Accept:
    libp2p_pubsub_validation_success.inc()
  of ValidationResult.Reject:
    libp2p_pubsub_validation_failure.inc()
  of ValidationResult.Ignore:
    libp2p_pubsub_validation_ignore.inc()

proc init*[PubParams: object | bool](
  P: typedesc[PubSub],
  switch: Switch,
  triggerSelf: bool = false,
  anonymize: bool = false,
  verifySignature: bool = true,
  sign: bool = true,
  msgIdProvider: MsgIdProvider = defaultMsgIdProvider,
  subscriptionValidator: SubscriptionValidator = nil,
  parameters: PubParams = false): P
  {.raises: [Defect, InitializationError].} =
  let pubsub =
    when PubParams is bool:
      P(switch: switch,
        peerInfo: switch.peerInfo,
        triggerSelf: triggerSelf,
        anonymize: anonymize,
        verifySignature: verifySignature,
        sign: sign,
        msgIdProvider: msgIdProvider,
        subscriptionValidator: subscriptionValidator,
        topicsHigh: int.high)
    else:
      P(switch: switch,
        peerInfo: switch.peerInfo,
        triggerSelf: triggerSelf,
        anonymize: anonymize,
        verifySignature: verifySignature,
        sign: sign,
        msgIdProvider: msgIdProvider,
        subscriptionValidator: subscriptionValidator,
        parameters: parameters,
        topicsHigh: int.high)

  proc peerEventHandler(peerId: PeerID, event: PeerEvent) {.async.} =
    if event.kind == PeerEventKind.Joined:
      pubsub.subscribePeer(peerId)
    else:
      pubsub.unsubscribePeer(peerId)

  switch.addPeerEventHandler(peerEventHandler, PeerEventKind.Joined)
  switch.addPeerEventHandler(peerEventHandler, PeerEventKind.Left)

  pubsub.knownTopics = KnownLibP2PTopicsSeq.toHashSet()

  pubsub.initPubSub()

  return pubsub

proc addObserver*(p: PubSub; observer: PubSubObserver) = p.observers[] &= observer

proc removeObserver*(p: PubSub; observer: PubSubObserver) =
  let idx = p.observers[].find(observer)
  if idx != -1:
    p.observers[].del(idx)
