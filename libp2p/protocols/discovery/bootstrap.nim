## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options, sequtils, tables
import chronos, chronicles
import discovery,
       ../utils/timedcache,
       ../../multiaddress,
       ../../protobuf/minprotobuf,
       ../../crypto/crypto,
       ../../connection,
       ../../peerinfo,
       ../../switch

const Codec* = "/eth2/bootstrap/1.0"
const MaxPeers* = 100

type
  Bootstrap* = ref object of Discovery
    switch: Switch
    bootstrapPeers*: seq[PeerInfo]
    maxPeers: int
    refreshTask: Future[void]
    running: bool
    sentPeers: TimedCache[PeerInfo]

proc encode*(msg: seq[PeerInfo]): ProtoBuffer =
  result = initProtoBuffer()
  var peerMsg: ProtoBuffer
  for m in msg:
    peerMsg = initProtoBuffer()
    peerMsg.write(initProtoField(1, m.publicKey().get().getBytes()))

    for ma in m.addrs:
      peerMsg.write(initProtoField(2, ma.data.buffer))

    peerMsg.finish()
    result.write(initProtoField(1, peerMsg))

  result.finish()

proc decode*(msg: seq[byte]): seq[PeerInfo] =
  var pb = initProtoBuffer(msg)
  while pb.enterSubMessage() > 0:
    var pubKey: PublicKey
    if pb.getValue(1, pubKey) < 0:
      warn "unable to read pubkey"
      continue

    var address = newSeq[byte]()
    var mas: seq[MultiAddress]
    while pb.getBytes(2, address) > 0:
      if len(address) != 0:
        var ma = MultiAddress.init(address)
        mas.add(ma)
        trace "read address bytes from message", address = ma
        address.setLen(0)
    result.add(PeerInfo.init(pubKey, mas))

method init(b: Bootstrap) =
  proc handler(conn: Connection, proto: string) {.async, gcsafe, closure.} =
    var msg = encode(toSeq(b.switch.connections.values).mapIt( it.peerInfo ))
    await conn.writeLp(msg.buffer)

  b.codec = Codec
  b.handler = handler

proc newBootstrap*(b: type[Bootstrap],
                   bootstrapPeers: seq[PeerInfo],
                   switch: Switch,
                   onNewPeers: NewPeersHandler,
                   interval: Duration = DefaultDiscoveryInterval,
                   peersTimeout: Duration = DefaultPeersTimeout,
                   maxPeers: int = MaxPeers): b =
  result = Bootstrap(switch: switch,
                     bootstrapPeers: bootstrapPeers,
                     interval: interval,
                     onNewPeers: onNewPeers,
                     peersTimeout: peersTimeout,
                     maxPeers: maxPeers)
  result.init()

proc getNewPeers(b: Bootstrap) {.async.} =
  await b.switch.onStarted.wait() # wait for the switch to start
  while b.running:
    var dials: seq[Future[Connection]]
    for p in b.bootstrapPeers:
      dials.add(b.switch.dial(p, Codec))

    await allFutures(dials)
    var bootstraps: seq[Connection]
    for d in dials:
      try:
        bootstraps.add(d.read())
      except CatchableError as exc:
        warn "unable to connect to bootstrap", exc = exc.msg
        continue

      for bootstrap in bootstraps:
        let msg = await bootstrap.readLp()
        await bootstrap.close()
        for p in msg.decode():
          b.peers.put(p.id, p)
          await b.onNewPeers(b.getPeers) # notify listeners of new peers

    await sleepAsync(b.interval)

method start*(b: Bootstrap) {.async.} =
  b.running = true
  b.refreshTask = b.getNewPeers()

method stop*(b: Bootstrap) {.async.} =
  b.running = false
  await b.refreshTask
