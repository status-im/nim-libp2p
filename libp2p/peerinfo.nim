## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import options, sequtils, hashes
import chronos, chronicles
import peerid, multiaddress, crypto/crypto

export peerid, multiaddress, crypto

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
    protoVersion*: string
    agentVersion*: string
    secure*: string
    case keyType*: KeyType:
    of HasPrivate:
      privateKey*: PrivateKey
    of HasPublic:
      key: Option[PublicKey]

func shortLog*(p: PeerInfo): auto =
  (
    peerId: $p.peerId,
    addrs: mapIt(p.addrs, $it),
    protocols: mapIt(p.protocols, $it),
    protoVersion: p.protoVersion,
    agentVersion: p.agentVersion,
  )
chronicles.formatIt(PeerInfo): shortLog(it)

template postInit(peerinfo: PeerInfo,
                  addrs: openarray[MultiAddress],
                  protocols: openarray[string]) =
  if len(addrs) > 0:
    peerinfo.addrs = @addrs
  if len(protocols) > 0:
    peerinfo.protocols = @protocols

proc init*(p: typedesc[PeerInfo],
           key: PrivateKey,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo
           {.raises: [Defect, ResultError[cstring]].} =
  result = PeerInfo(keyType: HasPrivate, peerId: PeerID.init(key).tryGet(),
                    privateKey: key)
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           peerId: PeerID,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo =
  result = PeerInfo(keyType: HasPublic, peerId: peerId)
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           peerId: string,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.
           raises: [Defect, ResultError[cstring]].} =
  result = PeerInfo(keyType: HasPublic, peerId: PeerID.init(peerId).tryGet())
  result.postInit(addrs, protocols)

proc init*(p: typedesc[PeerInfo],
           key: PublicKey,
           addrs: openarray[MultiAddress] = [],
           protocols: openarray[string] = []): PeerInfo {.
           raises: [Defect, ResultError[cstring]].}=
  result = PeerInfo(keyType: HasPublic,
                    peerId: PeerID.init(key).tryGet(),
                    key: some(key))

  result.postInit(addrs, protocols)

proc publicKey*(p: PeerInfo): Option[PublicKey] {.
    raises: [Defect, ResultError[CryptoError]].} =
  if p.keyType == HasPublic:
    if p.peerId.hasPublicKey():
      var pubKey: PublicKey
      if p.peerId.extractPublicKey(pubKey):
        result = some(pubKey)
    elif p.key.isSome:
      result = p.key
  else:
    result = some(p.privateKey.getKey().tryGet())

func hash*(p: PeerInfo): Hash =
  cast[pointer](p).hash
