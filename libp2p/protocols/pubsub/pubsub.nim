## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import tables, sequtils, sets
import chronos, chronicles
import pubsubpeer,
       rpc/messages,
       ../protocol,
       ../../connection,
       ../../peerinfo

export PubSubPeer
export PubSubObserver

logScope:
  topic = "PubSub"

type
  TopicHandler* = proc(topic: string,
                       data: seq[byte]): Future[void] {.gcsafe.}

  ValidatorHandler* = proc(topic: string,
                           message: Message): Future[bool] {.gcsafe, closure.}

  TopicPair* = tuple[topic: string, handler: TopicHandler]

  Topic* = object
    name*: string
    handler*: seq[TopicHandler]

  PubSub* = ref object of LPProtocol
    peerInfo*: PeerInfo               # this peer's info
    topics*: Table[string, Topic]     # local topics
    peers*: Table[string, PubSubPeer] # peerid to peer map
    triggerSelf*: bool                # trigger own local handler on publish
    verifySignature*: bool            # enable signature verification
    sign*: bool                       # enable message signing
    cleanupLock: AsyncLock
    validators*: Table[string, HashSet[ValidatorHandler]]
    observers: ref seq[PubSubObserver] # ref as in smart_ptr

proc sendSubs*(p: PubSub,
               peer: PubSubPeer,
               topics: seq[string],
               subscribe: bool) {.async.} =
  ## send subscriptions to remote peer
  trace "sending subscriptions", peer = peer.id,
                                 subscribe = subscribe,
                                 topicIDs = topics

  var msg: RPCMsg
  for t in topics:
    trace "sending topic", peer = peer.id,
                           subscribe = subscribe,
                           topicName = t
    msg.subscriptions.add(SubOpts(topic: t, subscribe: subscribe))

  await peer.send(@[msg])

method subscribeTopic*(p: PubSub,
                       topic: string,
                       subscribe: bool,
                       peerId: string) {.base.} =
  discard

method rpcHandler*(p: PubSub,
                   peer: PubSubPeer,
                   rpcMsgs: seq[RPCMsg]) {.async, base.} =
  ## handle rpc messages
  trace "processing RPC message", peer = peer.id, msgs = rpcMsgs.len

  for m in rpcMsgs:                                # for all RPC messages
    trace "processing messages", msg = m.shortLog
    if m.subscriptions.len > 0:                    # if there are any subscriptions
      for s in m.subscriptions:                    # subscribe/unsubscribe the peer for each topic
        p.subscribeTopic(s.topic, s.subscribe, peer.id)

method handleDisconnect*(p: PubSub, peer: PubSubPeer) {.async, base.} =
  ## handle peer disconnects
  if peer.id in p.peers:
    p.peers.del(peer.id)

proc cleanUpHelper(p: PubSub, peer: PubSubPeer) {.async.} =
  await p.cleanupLock.acquire()
  if peer.refs == 0:
    await p.handleDisconnect(peer)

  peer.refs.dec() # decrement refcount
  p.cleanupLock.release()

proc getPeer(p: PubSub,
             peerInfo: PeerInfo,
             proto: string): PubSubPeer =
  if peerInfo.id in p.peers:
    result = p.peers[peerInfo.id]
    return

  # create new pubsub peer
  let peer = newPubSubPeer(peerInfo, proto)
  trace "created new pubsub peer", peerId = peer.id

  p.peers[peer.id] = peer
  peer.refs.inc # increment reference cound
  peer.observers = p.observers
  result = peer

proc internalClenaup(p: PubSub, conn: Connection) {.async.} =
  # handle connection close
  if conn.closed:
    return

  var peer = p.getPeer(conn.peerInfo, p.codec)
  await conn.closeEvent.wait()
  trace "connection closed, cleaning up peer", peer = conn.peerInfo.id
  await p.cleanUpHelper(peer)

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

  proc handler(peer: PubSubPeer, msgs: seq[RPCMsg]) {.async.} =
    # call pubsub rpc handler
    await p.rpcHandler(peer, msgs)

  let peer = p.getPeer(conn.peerInfo, proto)
  let topics = toSeq(p.topics.keys)
  if topics.len > 0:
    await p.sendSubs(peer, topics, true)

  peer.handler = handler
  await peer.handle(conn) # spawn peer read loop
  trace "pubsub peer handler ended, cleaning up"
  await p.internalClenaup(conn)

method subscribeToPeer*(p: PubSub,
                        conn: Connection) {.base, async.} =
  var peer = p.getPeer(conn.peerInfo, p.codec)
  trace "setting connection for peer", peerId = conn.peerInfo.id
  if not peer.isConnected:
    peer.conn = conn

  asyncCheck p.internalClenaup(conn)

method unsubscribe*(p: PubSub,
                    topics: seq[TopicPair]) {.base, async.} =
  ## unsubscribe from a list of ``topic`` strings
  for t in topics:
    for i, h in p.topics[t.topic].handler:
      if h == t.handler:
        p.topics[t.topic].handler.del(i)

method unsubscribe*(p: PubSub,
                    topic: string,
                    handler: TopicHandler): Future[void] {.base.} =
  ## unsubscribe from a ``topic`` string
  result = p.unsubscribe(@[(topic, handler)])

method subscribe*(p: PubSub,
                  topic: string,
                  handler: TopicHandler) {.base, async.} =
  ## subscribe to a topic
  ##
  ## ``topic``   - a string topic to subscribe to
  ##
  ## ``handler`` - is a user provided proc
  ##               that will be triggered
  ##               on every received message
  ##
  if topic notin p.topics:
    trace "subscribing to topic", name = topic
    p.topics[topic] = Topic(name: topic)

  p.topics[topic].handler.add(handler)

  for peer in p.peers.values:
    await p.sendSubs(peer, @[topic], true)

method publish*(p: PubSub,
                topic: string,
                data: seq[byte]) {.base, async.} =
  ## publish to a ``topic``
  if p.triggerSelf and topic in p.topics:
    for h in p.topics[topic].handler:
      trace "triggering handler", topicID = topic
      try:
        await h(topic, data)
      except LPStreamEOFError:
        trace "Ignoring EOF while writing"
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        # TODO these exceptions are ignored since it's likely that if writes are
        #      are failing, the underlying connection is already closed - this needs
        #      more cleanup though
        debug "Could not write to pubsub connection", msg = exc.msg

method initPubSub*(p: PubSub) {.base.} =
  ## perform pubsub initializaion
  p.observers = new(seq[PubSubObserver])

method start*(p: PubSub) {.async, base.} =
  ## start pubsub
  discard

method stop*(p: PubSub) {.async, base.} =
  ## stopt pubsub
  discard

method addValidator*(p: PubSub,
                     topic: varargs[string],
                     hook: ValidatorHandler) {.base.} =
  for t in topic:
    if t notin p.validators:
      p.validators[t] = initHashSet[ValidatorHandler]()

    trace "adding validator for topic", topicId = t
    p.validators[t].incl(hook)

method removeValidator*(p: PubSub,
                        topic: varargs[string],
                        hook: ValidatorHandler) {.base.} =
  for t in topic:
    if t in p.validators:
      p.validators[t].excl(hook)

method validate*(p: PubSub, message: Message): Future[bool] {.async, base.} =
  var pending: seq[Future[bool]]
  trace "about to validate message"
  for topic in message.topicIDs:
    trace "looking for validators on topic", topicID = topic,
                                             registered = toSeq(p.validators.keys)
    if topic in p.validators:
      trace "running validators for topic", topicID = topic
      # TODO: add timeout to validator
      pending.add(p.validators[topic].mapIt(it(topic, message)))

  let futs = await allFinished(pending)
  result = futs.allIt(not it.failed and it.read())

proc newPubSub*(P: typedesc[PubSub],
                peerInfo: PeerInfo,
                triggerSelf: bool = false,
                verifySignature: bool = true,
                sign: bool = true): P =
  result = P(peerInfo: peerInfo,
             triggerSelf: triggerSelf,
             verifySignature: verifySignature,
             sign: sign,
             cleanupLock: newAsyncLock())
  result.initPubSub()

proc addObserver*(p: PubSub; observer: PubSubObserver) = p.observers[] &= observer

proc removeObserver*(p: PubSub; observer: PubSubObserver) =
  let idx = p.observers[].find(observer)
  if idx != -1:
    p.observers[].del(idx)
