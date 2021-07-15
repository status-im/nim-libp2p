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

logScope:
  topics = "libp2p transport"

type
  TransportError* = object of LPError
  TransportClosedError* = object of TransportError

  Transport* = ref object of RootObj
    ma*: Multiaddress
    running*: bool
    upgrader*: Upgrade

proc newTransportClosedError*(parent: ref Exception = nil): ref LPError =
  newException(TransportClosedError,
    "Transport closed, no more connections!", parent)

method start*(
  self: Transport,
  ma: MultiAddress): Future[void] {.base, async.} =
  ## start the transport
  ##

  trace "starting transport", address = $ma
  self.ma = ma
  self.running = true

method stop*(self: Transport): Future[void] {.base, async.} =
  ## stop and cleanup the transport
  ## including all outstanding connections
  ##

  trace "stopping transport", address = $self.ma
  self.running = false

method accept*(self: Transport): Future[Connection]
               {.base, gcsafe.} =
  ## accept incoming connections
  ##

  doAssert(false, "Not implemented!")

method dial*(
  self: Transport,
  address: MultiAddress): Future[Connection] {.base, gcsafe.} =
  ## dial a peer
  ##

  doAssert(false, "Not implemented!")

method upgradeIncoming*(
  self: Transport,
  conn: Connection): Future[void] {.base, gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  self.upgrader.upgradeIncoming(conn)

method upgradeOutgoing*(
  self: Transport,
  conn: Connection): Future[Connection] {.base, gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  self.upgrader.upgradeOutgoing(conn)

method handles*(
  self: Transport,
  address: MultiAddress): bool {.base, gcsafe.} =
  ## check if transport supports the multiaddress
  ##

  # by default we skip circuit addresses to avoid
  # having to repeat the check in every transport
  if address.protocols.isOk:
    return address.protocols
    .get()
    .filterIt(
      it == multiCodec("p2p-circuit")
    ).len == 0
