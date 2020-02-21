import unittest, sequtils, options
import chronos, chronicles
import utils,
       ../../libp2p/[switch,
                     crypto/crypto,
                     protocols/kademlia/kademlia,
                     protocols/kademlia/kadpeer,
                     protocols/kademlia/rpc/messages,
                     protocols/kademlia/rpc/protobuf]

logScope:
  topic = "testKademlia"

suite "Kademlia":
  test "Kademlia encode and decode ping":

    # Testing encoding and decoding ping
    var messageType = MessageType(strtype: "PING")
    var pingMessage = RPCMsg(messageType: messageType)
    debug "pingMessage", msg = pingMessage

    var encodedPing = encodeRpcMsg(pingMessage)
    debug "pingMessage encoded", encoded = encodedPing

    var decodedPing = decodeRpcMsg(encodedPing.buffer)
    debug "pingMessage decoded", decoded = decodedPing

    check:
      decodedPing.messageType.strtype == "PING"

  test "Kademlia encode and decode find node":

    # Example peer
    var pstr = "Qmdxy8GAu1pvi35xBAie9sMpMN4G9p6GK6WCNbSCDCDgyp"
    var pid = PeerID.init(pstr)
    debug "peer id", id = pid.pretty

    # Testing encoding and decoding find node
    var messageType = MessageType(strtype: "FIND_NODE")
    var key = Key(id: pid.getBytes())
    var findNodeMessage = RPCMsg(messageType: messageType, key: key)
    debug "findNode", msg = findNodeMessage

    var encodedFindNode = encodeRpcMsg(findNodeMessage)
    debug "findNode encoded", encoded = encodedFindNode

    var decodedFindNode = decodeRpcMsg(encodedFindNode.buffer)
    debug "findNode decoded", decoded = decodedFindNode

    var decodedId =  PeerID.init(decodedFindNode.key.id)
    debug "findNode decoded id", id = decodedId.pretty

    check:
      pstr == decodedId.pretty

  test "Kademlia encode and decode find node reply":

    # Example peer
    var pstr = "Qmdxy8GAu1pvi35xBAie9sMpMN4G9p6GK6WCNbSCDCDgyp"
    var pid = PeerID.init(pstr)

    # Adding self to closest peers just as test
    debug "peer id", id = pid.pretty
    var addrs: seq[seq[byte]]
    addrs.add(pid.getBytes())
    var p = Peer(id: pid.getBytes(), addrs: addrs)

    var peers: seq[Peer]
    peers.add(p)
    var closerPeers = CloserPeers(peers: peers)
    var messageType = MessageType(strtype: "FIND_NODE_REPLY")
    var key = Key(id: pid.getBytes())
    # Testing encoding and decoding find node reply
    var findNodeReplyMessage = RPCMsg(messageType: messageType,
                                      key: key,
                                      closerPeers: some(closerPeers))
    debug "findNodeReply", msg = findNodeReplyMessage

    var encodedFindNodeReply = encodeRpcMsg(findNodeReplyMessage)
    debug "findNodeReply encoded", encoded = encodedFindNodeReply

    echo "*** decoding"

    var decodedFindNodeReply = decodeRpcMsg(encodedFindNodeReply.buffer)
    debug "findNodeReply decoded", decoded = decodedFindNodeReply

    var decodedReplyId = PeerID.init(decodedFindNodeReply.key.id)
    debug "findNodeReply decoded id", id = decodedReplyId.pretty

    var someClose = decodedFindNodeReply.closerPeers.get.peers[0].id
    var someCloseNode = PeerID.init(someClose)
    debug "findNodeReply decoded someCloseNode id", id = someCloseNode.pretty

    check:
      decodedReplyId.pretty == someCloseNode.pretty


  test "Kademlia basic ping":
    proc runTests(): Future[bool] {.async.} =
      var completionFut = newFuture[bool]()
      proc handler(data: RPCMsg) {.async, gcsafe.} =
        debug "Basic ping handler", data=data
        completionFut.complete(true)

      # TODO: Ensure these nodes have right characteristics
      var nodes = generateNodes(2)
      var awaiters: seq[Future[void]]
      awaiters.add((await nodes[0].start())) # Alice
      awaiters.add((await nodes[1].start())) # Bob

      await listenAllNodes(nodes)
      # XXX: Unclear if this is right abstraction, ping/find_node
      # Equivalent 1-1 messages, vs pub/sub which are 1:N/N:1
      await nodes[1].listenForPing(handler)
      await sleepAsync(1000.millis)

      # This can probably be peer ID only, then look up table etc
      await nodes[0].ping(nodes[1].peerInfo)

      result = await completionFut
      await allFutures(nodes[0].stop(), nodes[1].stop())
      await allFutures(awaiters)

    check:
      waitFor(runTests()) == true
#
#  test "Kademlia add contact (XXX)":
#    proc runTests(): Future[bool] {.async.} =
#      var completionFut = newFuture[bool]()
#      proc handler(data: string) {.async, gcsafe.} =
#        echo("Hit handler in kademlia ping ", data)
#        completionFut.complete(true)
#
#      # TODO: Ensure these nodes have right characteristics
#      var nodes = generateNodes(2)
#      var awaiters: seq[Future[void]]
#      awaiters.add((await nodes[0].start())) # Alice
#      awaiters.add((await nodes[1].start())) # Bob
#
#      # XXX: Strictly speaking only A listens to B here
#      await listenAllNodes(nodes)
#
#      # XXX: Only adding contact and printing, no actual assertion
#      discard nodes[0].addContact(nodes[1].peerInfo)
#      completionFut.complete(true)
#
#      result = await completionFut
#      await allFutures(nodes[0].stop(), nodes[1].stop())
#      await allFutures(awaiters)
#
#    check:
#      waitFor(runTests()) == true
#
  test "Kademlia find node (WIP)":
    proc runTests(): Future[bool] {.async.} =
      var completionFut = newFuture[bool]()
      # XXX: handler signature
      # TODO: Convert Peer->KadPeer
      proc handler(data: seq[KadPeer]) {.async, gcsafe.} =
        debug "Find node RPC hit", data=data
        completionFut.complete(true)

      # TODO: Ensure these nodes have right characteristics
      var nodes = generateNodes(6)
      var awaiters: seq[Future[void]]
      awaiters.add((await nodes[0].start())) # Alice
      awaiters.add((await nodes[1].start())) # Bob
      awaiters.add((await nodes[2].start())) # Charlie

      awaiters.add((await nodes[3].start())) # D
      awaiters.add((await nodes[4].start())) # E
      awaiters.add((await nodes[5].start())) # F

      # XXX: Strictly speaking only A listens to B here
      # We need peer info from connection via switch here
      # XXX: Figure out how this works with addPeer, etc
      await listenAllNodes(nodes)

      # TODO: Replace with better here
      # XXX: Only adding contact and printing, no actual assertion
      discard nodes[0].addContact(nodes[1].peerInfo)

      # More add contacts
      # Assume Bob is already connected to Charlie, and D..F
      discard nodes[1].addContact(nodes[2].peerInfo)
      discard nodes[1].addContact(nodes[3].peerInfo)
      discard nodes[1].addContact(nodes[4].peerInfo)
      discard nodes[1].addContact(nodes[5].peerInfo)

      await nodes[1].listenForFindNode(handler)
      # TODO: Not what we want, we want to wait for response

      #await nodes[1].listenForFindNode()
      #completionFut.complete(true)

      debug "finding node (self)", self = nodes[0].peerInfo.peerId
      discard nodes[0].iterativeFindNode(nodes[0].peerInfo.peerId)
      # XXX

      # XXX: Unclear if this is right abstraction, ping/find_node
      # Equivalent 1-1 messages, vs pub/sub which are 1:N/N:1
      #await nodes[1].listenForPing(handler)
      #

      await sleepAsync(1000.millis)
      debug "Awaiting response from find node request"
      await sleepAsync(3000.millis)

      #completionFut.complete(true)

      # This can probably be peer ID only, then look up table etc
      #await nodes[0].ping(nodes[1].peerInfo)

      result = await completionFut
      await allFutures(nodes[0].stop(),
                       nodes[1].stop(),
                       nodes[2].stop(),
                       nodes[3].stop(),
                       nodes[4].stop(),
                       nodes[5].stop())
      await allFutures(awaiters)

    check:
      waitFor(runTests()) == true



# TODO: test "Kademlia basic find node":
# HERE ATM. Basic add contact, then find node. Finally.
