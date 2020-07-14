import unittest

{.used.}

import ../../libp2p/[peerid, peerinfo,
                     crypto/crypto,
                     protocols/pubsub/rpc/message,
                     protocols/pubsub/rpc/messages]

let rng = newRng()

suite "Message":
  test "signature":
    var seqno = 11'u64
    let
      peer = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
      msg = Message.init(peer, @[], "topic", seqno, sign = true)

    check verify(msg, peer)
