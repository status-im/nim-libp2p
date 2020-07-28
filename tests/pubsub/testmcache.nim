{.used.}

import unittest, options, sets, sequtils, bearssl
import stew/byteutils
import ../../libp2p/[peerid,
                     crypto/crypto,
                     protocols/pubsub/mcache,
                     protocols/pubsub/rpc/message,
                     protocols/pubsub/rpc/messages]

var rng = newRng()

proc randomPeerID(): PeerID =
  PeerID.init(PrivateKey.random(ECDSA, rng[]).get()).get()

suite "MCache":
  test "put/get":
    var mCache = newMCache(3, 5)
    var msg = Message(fromPeer: randomPeerID().data, seqno: "12345".toBytes())
    let msgId = defaultMsgIdProvider(msg)
    mCache.put(msgId, msg)
    check mCache.get(msgId).isSome and mCache.get(msgId).get() == msg

  test "window":
    var mCache = newMCache(3, 5)

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["foo"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    for i in 0..<5:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["bar"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    var mids = mCache.window("foo")
    check mids.len == 3

    var id = toSeq(mids)[0]
    check mCache.get(id).get().topicIDs[0] == "foo"

  test "shift - shift 1 window at a time":
    var mCache = newMCache(1, 5)

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["foo"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    mCache.shift()
    check mCache.window("foo").len == 0

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["bar"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    mCache.shift()
    check mCache.window("bar").len == 0

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["baz"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    mCache.shift()
    check mCache.window("baz").len == 0

  test "shift - 2 windows at a time":
    var mCache = newMCache(1, 5)

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["foo"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["bar"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    for i in 0..<3:
      var msg = Message(fromPeer: randomPeerID().data,
                        seqno: "12345".toBytes(),
                        topicIDs: @["baz"])
      mCache.put(defaultMsgIdProvider(msg), msg)

    mCache.shift()
    check mCache.window("foo").len == 0

    mCache.shift()
    check mCache.window("bar").len == 0
