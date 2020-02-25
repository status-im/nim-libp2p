## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options
import chronos
import peer, multiaddress, crypto/crypto

## A peer can be constructed in one of tree ways:
## 1) A local peer with a private key
## 2) A remote peer with a PeerID and it's public key stored
## in the ``id`` itself
## 3) A remote peer with a standalone public key, that isn't
## encoded in the ``id``
##

type
  KeyType* = enum
    HasPrivate,
    HasPublic

  PeerInfo* = ref object of RootObj
    peerId*: PeerID
    addrs*: seq[MultiAddress]
    protocols*: seq[string]
    lifefut: Future[void]
    case keyType*: KeyType:
    of HasPrivate:
      privateKey*: PrivateKey
    of HasPublic:
      key: Option[PublicKey]

template postInit(peerinfo: PeerInfo,
                  addrs: openarray[MultiAddress],
                  protocols: openarray[string]) =
  if len(addrs) > 0:
    peerinfo.addrs = @addrs
  if len(protocols) > 0:
    peerinfo.protocols = @protocols
  peerinfo.lifefut = newFuture[void]("libp2p.peerinfo.lifetime")

proc init*(p: typedesc[PeerInfo],
           key: PrivateKey,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.inline.} =
  result = PeerInfo(keyType: HasPrivate, peerId: PeerID.init(key),
                    privateKey: key)
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           peerId: PeerID,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.inline.} =
  result = PeerInfo(keyType: HasPublic, peerId: peerId)
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           peerId: string,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.inline.} =
  result = PeerInfo(keyType: HasPublic, peerId: PeerID.init(peerId))
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           key: PublicKey,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.inline.} =
  result = PeerInfo(keyType: HasPublic, peerId: PeerID.init(key),
                    key: some(key))
  result.postInit(addrs, protocols)

proc close*(p: PeerInfo) {.inline.} =
  p.lifefut.complete()

proc join*(p: PeerInfo): Future[void] {.inline.} =
  var retFuture = newFuture[void]()
  proc continuation(udata: pointer) {.gcsafe.} =
    if not(retFuture.finished()):
      retFuture.complete()
  proc cancellation(udata: pointer) {.gcsafe.} =
    p.lifefut.removeCallback(continuation)
  if p.lifefut.finished:
    retFuture.complete()
  else:
    p.lifefut.addCallback(continuation)
    retFuture.cancelCallback = cancellation
  return retFuture

proc isClosed*(p: PeerInfo): bool {.inline.} =
  result = p.lifefut.finished()

proc lifeFuture*(p: PeerInfo): Future[void] {.inline.} =
  result = p.lifefut

proc publicKey*(p: PeerInfo): Option[PublicKey] {.inline.} =
  if p.keyType == HasPublic:
    if p.peerId.hasPublicKey():
      var pubKey: PublicKey
      if p.peerId.extractPublicKey(pubKey):
        result = some(pubKey)
    elif p.key.isSome:
      result = p.key
  else:
    result = some(p.privateKey.getKey())

proc id*(p: PeerInfo): string {.inline.} =
  p.peerId.pretty()

proc `$`*(p: PeerInfo): string =
  result.add("PeerID: ")
  result.add(p.id & "\n")

  if p.addrs.len > 0:
    result.add("Peer Addrs: ")
    for a in p.addrs:
      result.add($a & "\n")

  if p.protocols.len > 0:
    result.add("Protocols: ")
    for proto in p.protocols:
      result.add(proto & "\n")
