## Nim-LibP2P
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

## This module implementes API for libp2p peer.

{.push raises: [Defect].}

import hashes
import nimcrypto/utils, stew/base58
import crypto/crypto, multicodec, multihash, vbuffer
import protobuf_serialization
import stew/results
export results

const
  maxInlineKeyLength* = 42

# TODO: add proper on disc serialization
# using peer-id protobuf format
type
  PeerID* {.protobuf2.} = object
    data* {.fieldNumber: 1.}: seq[byte]

  PeerIDError* = object of CatchableError

proc pretty*(pid: PeerID): string {.inline.} =
  ## Return base58 encoded ``pid`` representation.
  result = base58.encode(Base58, pid.data)

proc toBytes*(pid: PeerID, data: var openarray[byte]): int =
  ## Store PeerID ``pid`` to array of bytes ``data``.
  ##
  ## Returns number of bytes needed to store ``pid``.
  result = len(pid.data)
  if len(data) >= result and result > 0:
    copyMem(addr data[0], unsafeAddr pid.data[0], result)

proc getBytes*(pid: PeerID): seq[byte] {.inline.} =
  ## Return PeerID ``pid`` as array of bytes.
  result = pid.data

proc hex*(pid: PeerID): string {.inline.} =
  ## Returns hexadecimal string representation of ``pid``.
  if len(pid.data) > 0:
    result = toHex(pid.data)

proc len*(pid: PeerID): int {.inline.} =
  ## Returns length of ``pid`` binary representation.
  result = len(pid.data)

proc cmp*(a, b: PeerID): int =
  ## Compares two peer ids ``a`` and ``b``.
  ## Returns:
  ##
  ## | 0 iff a == b
  ## | < 0 iff a < b
  ## | > 0 iff a > b
  var i = 0
  var m = min(len(a.data), len(b.data))
  while i < m:
    result = ord(a.data[i]) - ord(b.data[i])
    if result != 0: return
    inc(i)
  result = len(a.data) - len(b.data)

proc `<=`*(a, b: PeerID): bool {.inline.} =
  (cmp(a, b) <= 0)

proc `<`*(a, b: PeerID): bool {.inline.} =
  (cmp(a, b) < 0)

proc `>=`*(a, b: PeerID): bool {.inline.} =
  (cmp(a, b) >= 0)

proc `>`*(a, b: PeerID): bool {.inline.} =
  (cmp(a, b) > 0)

proc `==`*(a, b: PeerID): bool {.inline.} =
  (cmp(a, b) == 0)

proc hash*(pid: PeerID): Hash {.inline.} =
  result = hash(pid.data)

proc validate*(pid: PeerID): bool =
  ## Validate check if ``pid`` is empty or not.
  if len(pid.data) > 0:
    result = MultiHash.validate(pid.data)

proc hasPublicKey*(pid: PeerID): bool =
  ## Returns ``true`` if ``pid`` is small enough to hold public key inside.
  if len(pid.data) > 0:
    var mh: MultiHash
    if MultiHash.decode(pid.data, mh).isOk:
      if mh.mcodec == multiCodec("identity"):
        result = true

proc extractPublicKey*(pid: PeerID, pubkey: var PublicKey): bool =
  ## Returns ``true`` if public key was successfully decoded from PeerID
  ## ``pid``and stored to ``pubkey``.
  ##
  ## Returns ``false`` otherwise.
  var mh: MultiHash
  if len(pid.data) > 0:
    if MultiHash.decode(pid.data, mh).isOk:
      if mh.mcodec == multiCodec("identity"):
        let length = len(mh.data.buffer)
        result = pubkey.init(mh.data.buffer.toOpenArray(mh.dpos, length - 1))

proc `$`*(pid: PeerID): string =
  ## Returns compact string representation of ``pid``.
  var spid = pid.pretty()
  if len(spid) <= 10:
    result = spid
  else:
    result = newStringOfCap(10)
    for i in 0..<2:
      result.add(spid[i])
    result.add("*")
    for i in (len(spid) - 6)..spid.high:
      result.add(spid[i])

proc init*(pid: var PeerID, data: openarray[byte]): bool =
  ## Initialize peer id from raw binary representation ``data``.
  ##
  ## Returns ``true`` if peer was successfully initialiazed.
  var p = PeerID(data: @data)
  if p.validate():
    pid = p
    result = true

proc init*(pid: var PeerID, data: string): bool =
  ## Initialize peer id from base58 encoded string representation.
  ##
  ## Returns ``true`` if peer was successfully initialiazed.
  var p = newSeq[byte](len(data) + 4)
  var length = 0
  if Base58.decode(data, p, length) == Base58Status.Success:
    p.setLen(length)
    var opid: PeerID
    shallowCopy(opid.data, p)
    if opid.validate():
      pid = opid
      result = true

proc init*(t: typedesc[PeerID], data: openarray[byte]): Result[PeerID, cstring] {.inline.} =
  ## Create new peer id from raw binary representation ``data``.
  var res: PeerID
  if not init(res, data):
    err("peerid: incorrect PeerID binary form")
  else:
    ok(res)

proc init*(t: typedesc[PeerID], data: string): Result[PeerID, cstring] {.inline.} =
  ## Create new peer id from base58 encoded string representation ``data``.
  var res: PeerID
  if not init(res, data):
    err("peerid: incorrect PeerID string")
  else:
    ok(res)

proc init*(t: typedesc[PeerID], pubkey: PublicKey): Result[PeerID, cstring] =
  ## Create new peer id from public key ``pubkey``.
  var pubraw = ? pubkey.getBytes().orError("peerid: failed to get bytes from given key")
  var mh: MultiHash
  if len(pubraw) <= maxInlineKeyLength:
    mh = ? MultiHash.digest("identity", pubraw)
  else:
    mh = ? MultiHash.digest("sha2-256", pubraw)
  ok(PeerID(data: mh.data.buffer))

proc init*(t: typedesc[PeerID], seckey: PrivateKey): Result[PeerID, cstring] {.inline.} =
  ## Create new peer id from private key ``seckey``.
  PeerID.init(? seckey.getKey().orError("invalid private key"))

proc match*(pid: PeerID, pubkey: PublicKey): bool {.inline.} =
  ## Returns ``true`` if ``pid`` matches public key ``pubkey``.
  let p = PeerID.init(pubkey)
  if p.isErr:
    false
  else:
    pid == p.get()

proc match*(pid: PeerID, seckey: PrivateKey): bool {.inline.} =
  ## Returns ``true`` if ``pid`` matches private key ``seckey``.
  let p = PeerID.init(seckey)
  if p.isErr:
    false
  else:
    pid == p.get()

## Serialization/Deserialization helpers

proc write*(vb: var VBuffer, pid: PeerID) {.inline.} =
  ## Write PeerID value ``peerid`` to buffer ``vb``.
  vb.writeSeq(pid.data)
