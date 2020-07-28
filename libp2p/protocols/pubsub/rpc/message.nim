## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import chronicles, metrics, stew/[byteutils, endians2], protobuf_serialization
import ./messages,
       ../../../peerid,
       ../../../peerinfo,
       ../../../crypto/crypto

export messages

logScope:
  topics = "pubsubmessage"

const PubSubPrefix = toBytes("libp2p-pubsub:")

declareCounter(libp2p_pubsub_sig_verify_success, "pubsub successfully validated messages")
declareCounter(libp2p_pubsub_sig_verify_failure, "pubsub failed validated messages")

func defaultMsgIdProvider*(m: Message): string =
  byteutils.toHex(m.seqno) & PeerID(data: m.fromPeer).pretty

proc sign*(msg: Message, p: PeerInfo): CryptoResult[seq[byte]] =
  ok((? p.privateKey.sign(PubSubPrefix & Protobuf.encode(msg))).getBytes())

proc verify*(m: Message, p: PeerInfo): bool =
  if m.signature.len > 0 and m.key.len > 0:
    var msg = m
    msg.signature = @[]
    msg.key = @[]

    var remote: Signature
    var key: PublicKey
    if remote.init(m.signature) and key.init(m.key):
      trace "verifying signature", remoteSignature = remote
      result = remote.verify(PubSubPrefix & Protobuf.encode(msg), key)

  if result:
    libp2p_pubsub_sig_verify_success.inc()
  else:
    libp2p_pubsub_sig_verify_failure.inc()

proc init*(
    T: type Message,
    p: PeerInfo,
    data: seq[byte],
    topic: string,
    seqno: uint64,
    sign: bool = true): Message {.gcsafe, raises: [CatchableError, Defect].} =
  result = Message(
    fromPeer: p.peerId.data,
    data: data,
    seqno: @(seqno.toBytesBE), # unefficient, fine for now
    topicIDs: @[topic])

  if sign and p.publicKey.isSome:
    result.signature = sign(result, p).tryGet()
    result.key =  p.publicKey.get().getBytes().tryGet()
