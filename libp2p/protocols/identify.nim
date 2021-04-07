## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options
import chronos, chronicles
import ../protobuf/minprotobuf,
       ../peerinfo,
       ../stream/connection,
       ../peerid,
       ../crypto/crypto,
       ../multiaddress,
       ../protocols/protocol,
       ../utility

logScope:
  topics = "libp2p identify"

const
  IdentifyCodec* = "/ipfs/id/1.0.0"
  IdentifyPushCodec* = "/ipfs/id/push/1.0.0"
  ProtoVersion* = "ipfs/0.1.0"
  AgentVersion* = "nim-libp2p/0.0.1"

#TODO: implement push identify, leaving out for now as it is not essential

type
  IdentifyError* = object of LPError
  IdentityNoMatchError* = object of IdentifyError
  IdentityInvalidMsgError* = object of IdentifyError

  IdentifyInfo* = object
    pubKey*: Option[PublicKey]
    addrs*: seq[MultiAddress]
    observedAddr*: Option[MultiAddress]
    protoVersion*: Option[string]
    agentVersion*: Option[string]
    protos*: seq[string]

  Identify* = ref object of LPProtocol
    peerInfo*: PeerInfo

proc encodeMsg*(peerInfo: PeerInfo, observedAddr: Multiaddress): ProtoBuffer =
  result = initProtoBuffer()
  result.write(1, peerInfo.publicKey.get().getBytes().tryGet())
  for ma in peerInfo.addrs:
    result.write(2, ma.data.buffer)
  for proto in peerInfo.protocols:
    result.write(3, proto)
  result.write(4, observedAddr.data.buffer)
  let protoVersion = ProtoVersion
  result.write(5, protoVersion)
  let agentVersion = if peerInfo.agentVersion.len <= 0:
    AgentVersion
  else:
    peerInfo.agentVersion
  result.write(6, agentVersion)
  result.finish()

proc decodeMsg*(buf: seq[byte]): Option[IdentifyInfo] =
  var
    iinfo: IdentifyInfo
    pubKey: PublicKey
    oaddr: MultiAddress
    protoVersion: string
    agentVersion: string

  var pb = initProtoBuffer(buf)

  let r1 = pb.getField(1, pubKey)
  let r2 = pb.getRepeatedField(2, iinfo.addrs)
  let r3 = pb.getRepeatedField(3, iinfo.protos)
  let r4 = pb.getField(4, oaddr)
  let r5 = pb.getField(5, protoVersion)
  let r6 = pb.getField(6, agentVersion)

  let res = r1.isOk() and r2.isOk() and r3.isOk() and
            r4.isOk() and r5.isOk() and r6.isOk()

  if res:
    if r1.get():
      iinfo.pubKey = some(pubKey)
    if r4.get():
      iinfo.observedAddr = some(oaddr)
    if r5.get():
      iinfo.protoVersion = some(protoVersion)
    if r6.get():
      iinfo.agentVersion = some(agentVersion)
    debug "decodeMsg: decoded message", pubkey = ($pubKey).shortLog,
          addresses = $iinfo.addrs, protocols = $iinfo.protos,
          observable_address = $iinfo.observedAddr,
          proto_version = $iinfo.protoVersion,
          agent_version = $iinfo.agentVersion
    some(iinfo)
  else:
    trace "decodeMsg: failed to decode received message"
    none[IdentifyInfo]()

proc newIdentify*(peerInfo: PeerInfo): Identify =
  new result
  result.peerInfo = peerInfo
  result.init()

method init*(p: Identify) =
  proc handle(conn: Connection, proto: string) {.async, gcsafe, closure.} =
    try:
      trace "handling identify request", conn
      var pb = encodeMsg(p.peerInfo, conn.observedAddr)
      await conn.writeLp(pb.buffer)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "exception in identify handler", exc = exc.msg, conn
    finally:
      trace "exiting identify handler", conn
      await conn.closeWithEOF()

  p.handler = handle
  p.codec = IdentifyCodec

proc identify*(p: Identify,
               conn: Connection,
               remotePeerInfo: PeerInfo): Future[IdentifyInfo] {.async, gcsafe.} =
  trace "initiating identify", conn
  var message = await conn.readLp(64*1024)
  if len(message) == 0:
    trace "identify: Empty message received!", conn
    raise newException(IdentityInvalidMsgError, "Empty message received!")

  let infoOpt = decodeMsg(message)
  if infoOpt.isNone():
    raise newException(IdentityInvalidMsgError, "Incorrect message received!")
  result = infoOpt.get()

  if not isNil(remotePeerInfo) and result.pubKey.isSome:
    let peer = PeerID.init(result.pubKey.get())
    if peer.isErr:
      raise newException(IdentityInvalidMsgError, $peer.error)
    else:
      if peer.get() != remotePeerInfo.peerId:
        trace "Peer ids don't match",
              remote = peer,
              local = remotePeerInfo.peerId

        raise newException(IdentityNoMatchError, "Peer ids don't match")

proc push*(p: Identify, conn: Connection) {.async.} =
  await conn.write(IdentifyPushCodec)
  var pb = encodeMsg(p.peerInfo, conn.observedAddr)
  await conn.writeLp(pb.buffer)
