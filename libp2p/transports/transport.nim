## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.
##

{.push raises: [Defect].}

import sequtils
import chronos, chronicles
import ../stream/connection,
       ../multiaddress,
       ../multicodec,
       ../upgrademngrs/upgrade
import ./session

logScope:
  topics = "libp2p transport"

type
  TransportError* = object of LPError
  TransportClosedError* = object of TransportError

  Transport* = ref object of RootObj
    ma*: Multiaddress
    running*: bool
    upgrader*: Upgrade
    multicodec*: MultiCodec

proc newTransportClosedError*(parent: ref Exception = nil): ref LPError =
  newException(TransportClosedError,
    "Transport closed, no more connections!", parent)

method initTransport*(self: Transport) {.base, gcsafe, locks: "unknown".} =
  ## perform protocol initialization
  ##

  discard

method start*(
  self: Transport,
  ma: MultiAddress): Future[void] {.base, async.} =
  ## start the transport
  ##

  self.ma = ma
  trace "starting transport", address = $ma

method stop*(self: Transport): Future[void] {.base, async.} =
  ## stop and cleanup the transport
  ## including all outstanding connections
  ##

  discard

method accept*(self: Transport): Future[Session] {.base, async.} =
  ## accept incoming session

  doAssert false # not implemented

proc acceptStream*(self: Transport): Future[Connection] {.async.} =
  ## accept incoming session, and its first stream
  ##

  let session = await self.accept()
  result = await session.getStream()

method dial*(self: Transport,
             address: MultiAddress): Future[Session] {.base, async.} =
  ## dial a peer

  doAssert false # not implemented

proc dialStream*(self: Transport,
                 address: MultiAddress): Future[Connection] {.async.} =
  ## dial a peer, and get a first stream
  ##

  let session = await self.dial(address)
  result = await session.getStream()

method upgradeIncoming*(
  self: Transport,
  conn: Connection): Future[void] {.base, gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  doAssert(false, "Not implemented!")

method upgradeOutgoing*(
  self: Transport,
  conn: Connection): Future[Connection] {.base, gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  doAssert(false, "Not implemented!")

method handles*(
  self: Transport,
  address: MultiAddress): bool {.base, gcsafe.} =
  ## check if transport supports the multiaddress
  ##

  # by default we skip circuit addresses to avoid
  # having to repeat the check in every transport
  if address.protocols.isOk:
    return address.protocols.get().filterIt( it == multiCodec("p2p-circuit") ).len == 0

method localAddress*(self: Transport): MultiAddress {.base, gcsafe.} =
  ## get the local address of the transport in case started with 0.0.0.0:0
  ##

  discard
