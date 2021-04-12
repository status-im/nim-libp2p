## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect, DialFailedError].}

import chronos
import peerid,
       stream/connection

export peerid, connection

type
  DialFailedError* = object of LPError
  Dial* = ref object of RootObj

method connect*(
  self: Dial,
  peerId: PeerID,
  addrs: seq[MultiAddress]) {.async, base.} =
  ## connect remote peer without negotiating
  ## a protocol
  ##

  doAssert(false, "Not implemented!")

method dial*(
  self: Dial,
  peerId: PeerID,
  protos: seq[string]): Future[Connection] {.async, base.} =
  ## create a protocol stream over an
  ## existing connection
  ##

  doAssert(false, "Not implemented!")

method dial*(
  self: Dial,
  peerId: PeerID,
  addrs: seq[MultiAddress],
  protos: seq[string]): Future[Connection] {.async, base.} =
  ## create a protocol stream and establish
  ## a connection if one doesn't exist already
  ##

  doAssert(false, "Not implemented!")
