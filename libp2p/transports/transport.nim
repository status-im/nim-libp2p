## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.
##

import sequtils
import chronos, chronicles
import ../stream/connection,
       ../multiaddress,
       ../multicodec

logScope:
  topics = "libp2p transport"

type
  TransportError* = object of LPError
  TransportClosedError* = object of TransportError

  Transport* = ref object of RootObj
    ma*: Multiaddress
    multicodec*: MultiCodec
    running*: bool

proc newTransportClosedError*(parent: ref Exception = nil): ref CatchableError =
  newException(TransportClosedError,
    "Transport closed, no more connections!", parent)

method initTransport*(t: Transport) {.base, gcsafe, locks: "unknown".} =
  ## perform protocol initialization
  ##

  discard

method start*(t: Transport, ma: MultiAddress) {.base, async.} =
  ## start the transport
  ##

  t.ma = ma
  trace "starting transport", address = $ma

method stop*(t: Transport) {.base, async.} =
  ## stop and cleanup the transport
  ## including all outstanding connections
  ##

  discard

method accept*(t: Transport): Future[Connection]
               {.base, async, gcsafe.} =
  ## accept incoming connections
  ##

  discard

method dial*(t: Transport,
             address: MultiAddress): Future[Connection]
             {.base, async, gcsafe.} =
  ## dial a peer
  ##

  discard

method upgrade*(t: Transport) {.base, async, gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  discard

method handles*(t: Transport, address: MultiAddress): bool {.base, gcsafe.} =
  ## check if transport supports the multiaddress
  ##

  # by default we skip circuit addresses to avoid
  # having to repeat the check in every transport
  address.protocols.tryGet().filterIt( it == multiCodec("p2p-circuit") ).len == 0

method localAddress*(t: Transport): MultiAddress {.base, gcsafe.} =
  ## get the local address of the transport in case started with 0.0.0.0:0
  ##

  discard
