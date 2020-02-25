## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos, sequtils
import ../protocol,
       ../../peerinfo,
       ../utils/timedcache

const DefaultDiscoveryInterval* = 10.seconds
const DefaultPeersTimeout* = 5.minutes

type
  NewPeersHandler* = proc(peers: seq[PeerInfo]): Future[void]

  Discovery* = ref object of LPProtocol
    onNewPeers*: NewPeersHandler # new peers handler
    interval*: Duration # how often to trigger peer discovery
    peers*: TimedCache[PeerInfo]

proc newDiscovery*(d: type[Discovery],
                   onNewPeers: NewPeersHandler,
                   interval: Duration = DefaultDiscoveryInterval,
                   peersTimeout: Duration = DefaultPeersTimeout): d =
  Discovery(onNewPeers: onNewPeers,
            interval: interval,
            peers: newTimedCache[PeerInfo]())

proc getPeers*(d: Discovery): seq[PeerInfo] =
  d.peers.entries().mapIt( it.val )

method start*(d: Discovery) {.base, async.} =
  doAssert(false, "Not implmented!")

method stop*(d: Discovery) {.base, async.} =
  doAssert(false, "Not implemented!")
