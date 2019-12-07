
import unittest
import ../libp2p/crypto/crypto,
       ../libp2p/peerinfo,
       ../libp2p/peer

suite "PeerInfo":
  test "Should init with private key":
    let seckey = PrivateKey.random(RSA)
    var peerInfo = PeerInfo.init(seckey)
    var peerId = PeerID.init(seckey)

    check peerId == peerInfo.peerId
    check seckey == peerInfo.privateKey
    check seckey.getKey == peerInfo.publicKey

  test "Should init with public key":
    let seckey = PrivateKey.random(RSA)
    var peerInfo = PeerInfo.init(seckey.getKey())
    var peerId = PeerID.init(seckey.getKey())

    check peerId == peerInfo.peerId
    check seckey.getKey == peerInfo.publicKey

  test "Should init from PeerId with public key":
    let seckey = PrivateKey.random(Ed25519)
    var peerInfo = PeerInfo.init(PeerID.init(seckey.getKey()))
    var peerId = PeerID.init(seckey.getKey())

    check peerId == peerInfo.peerId
    check seckey.getKey == peerInfo.publicKey

  test "Should throw on missing public key":
    proc throwsOnMissingPubKey() =
      let peerInfo = PeerInfo.init(PeerID.init(PrivateKey.random(RSA)))
      discard peerInfo.publicKey

    expect NoPublicKeyException:
      throwsOnMissingPubKey()

  test "Should allow assigning public key":
    let key = PrivateKey.random(RSA)

    let peerInfo = PeerInfo.init(PeerID.init(key))
    peerInfo.publicKey = key.getKey()
    check peerInfo.publicKey == key.getKey()

  test "Should throw on invalid public key assignement":
    proc throwsOnInvalidPubKey() =
      let validKey = PrivateKey.random(RSA)
      let invalidKey = PrivateKey.random(RSA)

      let peerInfo = PeerInfo.init(PeerID.init(validKey))
      peerInfo.publicKey = invalidKey.getKey()

    expect InvalidPublicKeyException:
      throwsOnInvalidPubKey()
