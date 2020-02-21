import options, tables
import chronos, chronicles
import ../../libp2p/standard_setup
export standard_setup

logScope:
  topic = "testKademlia utils"

proc generateNodes*(num: Natural): seq[Switch] =
  for i in 0..<num:
    result.add(newKadSwitch())

# XXX: Not really what we want, but OK
# Here all peers listen to all other nodes
# TODO: Break up
proc listenAllNodes*(nodes: seq[Switch]) {.async.} =
  trace "listenAllNodes"
  var dials: seq[Future[void]]
  for dialer in nodes:
    for node in nodes:
      if dialer.peerInfo.peerId != node.peerInfo.peerId:
        dials.add(dialer.listenToPeer(node.peerInfo))
  await sleepAsync(100.millis)
  await allFutures(dials)
