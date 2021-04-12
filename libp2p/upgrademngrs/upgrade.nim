## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[options, sequtils]
import pkg/[chronos, chronicles, metrics]

import ../stream/connection,
       ../protocols/secure/secure,
       ../protocols/identify,
       ../multistream,
       ../connmanager

export connmanager, connection, identify, secure, multistream

declarePublicCounter(libp2p_failed_upgrade, "peers failed upgrade")

logScope:
  topics = "libp2p upgrade"

type
  UpgradeFailedError* = object of LPError

  Upgrade* = ref object of RootObj
    ms*: MultistreamSelect
    identity*: Identify
    connManager*: ConnManager
    secureManagers*: seq[Secure]

method upgradeIncoming*(
  self: Upgrade,
  conn: Connection): Future[void] {.base.} =
  doAssert(false, "Not implemented!")

method upgradeOutgoing*(
  self: Upgrade,
  conn: Connection): Future[Connection] {.base.} =
  doAssert(false, "Not implemented!")

proc secure*(
  self: Upgrade,
  conn: Connection): Future[Connection] {.async, gcsafe.} =
  if self.secureManagers.len <= 0:
    raise newException(UpgradeFailedError, "No secure managers registered!")

  let codec = await self.ms.select(conn, self.secureManagers.mapIt(it.codec))
  if codec.len == 0:
    raise newException(UpgradeFailedError, "Unable to negotiate a secure channel!")

  trace "Securing connection", conn, codec
  let secureProtocol = self.secureManagers.filterIt(it.codec == codec)

  # ms.select should deal with the correctness of this
  # let's avoid duplicating checks but detect if it fails to do it properly
  doAssert(secureProtocol.len > 0)

  return await secureProtocol[0].secure(conn, true)

proc identify*(
  self: Upgrade,
  conn: Connection) {.async, gcsafe.} =
  ## identify the connection

  if (await self.ms.select(conn, self.identity.codec)):
    let info = await self.identity.identify(conn, conn.peerInfo)

    if info.pubKey.isNone and isNil(conn):
      raise newException(UpgradeFailedError,
        "no public key provided and no existing peer identity found")

    if isNil(conn.peerInfo):
      conn.peerInfo = PeerInfo.init(info.pubKey.get())

    if info.addrs.len > 0:
      conn.peerInfo.addrs = info.addrs

    if info.agentVersion.isSome:
      conn.peerInfo.agentVersion = info.agentVersion.get()

    if info.protoVersion.isSome:
      conn.peerInfo.protoVersion = info.protoVersion.get()

    if info.protos.len > 0:
      conn.peerInfo.protocols = info.protos

    trace "identified remote peer", conn, peerInfo = shortLog(conn.peerInfo)
