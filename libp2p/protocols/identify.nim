## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options
import chronos, chronicles, protobuf_serialization
import ../peerinfo,
       ../stream/connection,
       ../peerid,
       ../crypto/crypto,
       ../multiaddress,
       ../protocols/protocol,
       ../utility

logScope:
  topics = "identify"

const
  IdentifyCodec* = "/ipfs/id/1.0.0"
  IdentifyPushCodec* = "/ipfs/id/push/1.0.0"
  ProtoVersion* = "ipfs/0.1.0"
  AgentVersion* = "nim-libp2p/0.0.1"

#TODO: implement push identify, leaving out for now as it is not essential

type
  IdentityNoMatchError* = object of CatchableError
  IdentityInvalidMsgError* = object of CatchableError

  IdentifyInfo* = object
    pubKey*: Option[PublicKey]
    addrs*: seq[MultiAddress]
    observedAddr*: Option[MultiAddress]
    protoVersion*: Option[string]
    agentVersion*: Option[string]
    protos*: seq[string]

  Identify* = ref object of LPProtocol
    peerInfo*: PeerInfo

  SerializableIdentifyInfo {.protobuf2.} = object
    pubKey {.fieldNumber: 1.}: seq[byte]
    addresses {.fieldNumber: 2.}: seq[seq[byte]]
    protocols {.fieldNumber: 3.}: seq[string]
    observedAddrs {.fieldNumber: 4.}: seq[byte]
    version {.fieldNumber: 5, required.}: string
    agent {.fieldNumber: 6, required.}: string

proc encodeMsg*(peerInfo: PeerInfo, observedAddrs: Multiaddress): seq[byte] =
  var byteAddrs = newSeq[seq[byte]](peerInfo.addrs.len)
  for a in 0 ..< byteAddrs.len:
    byteAddrs[a] = peerInfo.addrs[a].data.buffer
  Protobuf.encode(SerializableIdentifyInfo(
    pubKey: peerInfo.publicKey.get().getBytes().tryGet(),
    addresses: byteAddrs,
    protocols: peerInfo.protocols,
    observedAddrs: observedAddrs.data.buffer,
    version: ProtoVersion,
    agent: AgentVersion
  ))

proc newIdentify*(peerInfo: PeerInfo): Identify =
  new result
  result.peerInfo = peerInfo
  result.init()

method init*(p: Identify) =
  proc handle(conn: Connection, proto: string) {.async, gcsafe, closure.} =
    try:
      defer:
        trace "exiting identify handler", oid = conn.oid
        await conn.close()

      trace "handling identify request", oid = conn.oid
      var pb = encodeMsg(p.peerInfo, conn.observedAddr)
      await conn.writeLp(pb)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "exception in identify handler", exc = exc.msg

  p.handler = handle
  p.codec = IdentifyCodec

proc identify*(p: Identify,
               conn: Connection,
               remotePeerInfo: PeerInfo): Future[IdentifyInfo] {.async, gcsafe.} =
  trace "initiating identify", peer = $conn
  var message = await conn.readLp(64*1024)
  if len(message) == 0:
    trace "identify: Empty message received!"
    raise newException(IdentityInvalidMsgError, "Empty message received!")

  try:
    let temp = Protobuf.decode(message, SerializableIdentifyInfo)
    if temp.pubKey.len > 0:
      var potentialPubKey: PublicKey
      if potentialPubKey.init(temp.pubKey):
        result.pubKey = some(potentialPubKey)

    for mAddr in temp.addresses:
      var ma = MultiAddress.init(mAddr)
      if ma.isOk():
        result.addrs.add(ma.get())
      else:
        raise newException(IdentityInvalidMsgError, "Incorrect message received!")

    if temp.observedAddrs.len > 0:
      var ma = MultiAddress.init(temp.observedAddrs)
      if ma.isOk():
        result.observedAddr = some(ma.get())
      else:
        raise newException(IdentityInvalidMsgError, "Incorrect message received!")

    if temp.version.len > 0:
      result.protoVersion = some(temp.version)
    if temp.agent.len > 0:
      result.agentVersion = some(temp.version)
    result.protos = temp.protocols
  except ProtobufReadError:
    raise newException(IdentityInvalidMsgError, "Incorrect message received!")

  if not isNil(remotePeerInfo) and result.pubKey.isSome:
    let peer = PeerID.init(result.pubKey.get())
    if peer.isErr:
      raise newException(IdentityInvalidMsgError, $peer.error)
    else:
      # do a string comaprison of the ids,
      # because that is the only thing we
      # have in most cases
      if peer.get() != remotePeerInfo.peerId:
        trace "Peer ids don't match",
              remote = peer.get().pretty(),
              local = remotePeerInfo.id

        raise newException(IdentityNoMatchError, "Peer ids don't match")

proc push*(p: Identify, conn: Connection) {.async.} =
  await conn.write(IdentifyPushCodec)
  var pb = encodeMsg(p.peerInfo, conn.observedAddr)
  await conn.writeLp(pb)
