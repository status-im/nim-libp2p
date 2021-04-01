import options, tables
import unittest
import chronos, chronicles, stew/byteutils
import helpers
import ../libp2p/[daemon/daemonapi,
                  protobuf/minprotobuf,
                  vbuffer,
                  multiaddress,
                  multicodec,
                  cid,
                  varint,
                  multihash,
                  builders,
                  peerid,
                  peerinfo,
                  switch,
                  stream/connection,
                  muxers/muxer,
                  crypto/crypto,
                  muxers/mplex/mplex,
                  muxers/muxer,
                  protocols/protocol,
                  protocols/identify,
                  transports/transport,
                  transports/tcptransport,
                  protocols/secure/secure,
                  protocols/pubsub/pubsub,
                  protocols/pubsub/floodsub,
                  protocols/pubsub/gossipsub]

type
  # TODO: Unify both PeerInfo structs
  NativePeerInfo = peerinfo.PeerInfo
  DaemonPeerInfo = daemonapi.PeerInfo

proc writeLp*(s: StreamTransport, msg: string | seq[byte]): Future[int] {.gcsafe.} =
  ## write lenght prefixed
  var buf = initVBuffer()
  buf.writeSeq(msg)
  buf.finish()
  result = s.write(buf.buffer)

proc readLp*(s: StreamTransport): Future[seq[byte]] {.async, gcsafe.} =
  ## read length prefixed msg
  var
    size: uint
    length: int
    res: VarintResult[void]
  result = newSeq[byte](10)

  for i in 0..<len(result):
    await s.readExactly(addr result[i], 1)
    res = LP.getUVarint(result.toOpenArray(0, i), length, size)
    if res.isOk():
      break
  res.expect("Valid varint")
  result.setLen(size)
  if size > 0.uint:
    await s.readExactly(addr result[0], int(size))

proc testPubSubDaemonPublish(gossip: bool = false, count: int = 1) {.async.} =
  var pubsubData = "TEST MESSAGE"
  var testTopic = "test-topic"
  var msgData = pubsubData.toBytes()

  var flags = {PSFloodSub}
  if gossip:
    flags = {PSGossipSub}

  let daemonNode = await newDaemonApi(flags)
  let daemonPeer = await daemonNode.identity()
  let nativeNode = newStandardSwitch(outTimeout = 5.minutes)

  let pubsub = if gossip:
      GossipSub.init(
        switch = nativeNode).PubSub
    else:
      FloodSub.init(
        switch = nativeNode).PubSub

  nativeNode.mount(pubsub)

  let awaiters = nativeNode.start()
  await pubsub.start()
  let nativePeer = nativeNode.peerInfo

  var finished = false
  var times = 0
  proc nativeHandler(topic: string, data: seq[byte]) {.async.} =
    let smsg = string.fromBytes(data)
    check smsg == pubsubData
    times.inc()
    if times >= count and not finished:
      finished = true

  let peer = NativePeerInfo.init(
    daemonPeer.peer,
    daemonPeer.addresses)
  await nativeNode.connect(peer.peerId, peer.addrs)

  await sleepAsync(1.seconds)
  await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)

  proc pubsubHandler(api: DaemonAPI,
                     ticket: PubsubTicket,
                     message: PubSubMessage): Future[bool] {.async.} =
    result = true # don't cancel subscription

  asyncDiscard daemonNode.pubsubSubscribe(testTopic, pubsubHandler)
  pubsub.subscribe(testTopic, nativeHandler)
  await sleepAsync(5.seconds)

  proc publisher() {.async.} =
    while not finished:
      await daemonNode.pubsubPublish(testTopic, msgData)
      await sleepAsync(500.millis)

  await wait(publisher(), 5.minutes) # should be plenty of time

  await nativeNode.stop()
  await pubsub.stop()
  await allFutures(awaiters)
  await daemonNode.close()

proc testPubSubNodePublish(gossip: bool = false, count: int = 1) {.async.} =
  var pubsubData = "TEST MESSAGE"
  var testTopic = "test-topic"
  var msgData = pubsubData.toBytes()

  var flags = {PSFloodSub}
  if gossip:
    flags = {PSGossipSub}

  let daemonNode = await newDaemonApi(flags)
  let daemonPeer = await daemonNode.identity()
  let nativeNode = newStandardSwitch(outTimeout = 5.minutes)

  let pubsub = if gossip:
      GossipSub.init(
        switch = nativeNode).PubSub
    else:
      FloodSub.init(
        switch = nativeNode).PubSub

  nativeNode.mount(pubsub)

  let awaiters = nativeNode.start()
  await pubsub.start()
  let nativePeer = nativeNode.peerInfo

  let peer = NativePeerInfo.init(
    daemonPeer.peer,
    daemonPeer.addresses)
  await nativeNode.connect(peer)

  await sleepAsync(1.seconds)
  await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)

  var times = 0
  var finished = false
  proc pubsubHandler(api: DaemonAPI,
                     ticket: PubsubTicket,
                     message: PubSubMessage): Future[bool] {.async.} =
    let smsg = string.fromBytes(message.data)
    check smsg == pubsubData
    times.inc()
    if times >= count and not finished:
      finished = true
    result = true # don't cancel subscription

  discard await daemonNode.pubsubSubscribe(testTopic, pubsubHandler)
  proc nativeHandler(topic: string, data: seq[byte]) {.async.} = discard
  pubsub.subscribe(testTopic, nativeHandler)
  await sleepAsync(5.seconds)

  proc publisher() {.async.} =
    while not finished:
      discard await pubsub.publish(testTopic, msgData)
      await sleepAsync(500.millis)

  await wait(publisher(), 5.minutes) # should be plenty of time

  check finished
  await nativeNode.stop()
  await pubsub.stop()
  await allFutures(awaiters)
  await daemonNode.close()

suite "Interop":
  # TODO: chronos transports are leaking,
  # but those are tracked for both the daemon
  # and libp2p, so not sure which one it is,
  # need to investigate more
  # teardown:
  #   checkTrackers()

  # TODO: this test is failing sometimes on windows
  # For some reason we receive EOF before test 4 sometimes
  asyncTest "native -> daemon multiple reads and writes":
    var protos = @["/test-stream"]

    let nativeNode = newStandardSwitch(
      secureManagers = [SecureProtocol.Noise],
      outTimeout = 5.minutes)

    let awaiters = await nativeNode.start()
    let daemonNode = await newDaemonApi()
    let daemonPeer = await daemonNode.identity()

    var testFuture = newFuture[void]("test.future")
    proc daemonHandler(api: DaemonAPI, stream: P2PStream) {.async.} =
      check string.fromBytes(await stream.transp.readLp()) == "test 1"
      discard await stream.transp.writeLp("test 2")
      check string.fromBytes(await stream.transp.readLp()) == "test 3"
      discard await stream.transp.writeLp("test 4")
      testFuture.complete()

    await daemonNode.addHandler(protos, daemonHandler)
    let conn = await nativeNode.dial(NativePeerInfo.init(daemonPeer.peer,
                                                          daemonPeer.addresses),
                                                          protos[0])
    await conn.writeLp("test 1")
    check "test 2" == string.fromBytes((await conn.readLp(1024)))

    await conn.writeLp("test 3")
    check "test 4" == string.fromBytes((await conn.readLp(1024)))

    await wait(testFuture, 10.secs)

    await nativeNode.stop()
    await daemonNode.close()
    await allFutures(awaiters)

    await sleepAsync(1.seconds)

  asyncTest "native -> daemon connection":
    var protos = @["/test-stream"]
    var test = "TEST STRING"
    # We are preparing expect string, which should be prefixed with varint
    # length and do not have `\r\n` suffix, because we going to use
    # readLine().
    var buffer = initVBuffer()
    buffer.writeSeq(test & "\r\n")
    buffer.finish()
    var expect = newString(len(buffer) - 2)
    copyMem(addr expect[0], addr buffer.buffer[0], len(expect))

    let nativeNode = newStandardSwitch(
      secureManagers = [SecureProtocol.Noise],
      outTimeout = 5.minutes)

    let awaiters = await nativeNode.start()

    let daemonNode = await newDaemonApi()
    let daemonPeer = await daemonNode.identity()

    var testFuture = newFuture[string]("test.future")
    proc daemonHandler(api: DaemonAPI, stream: P2PStream) {.async.} =
      # We should perform `readLp()` instead of `readLine()`. `readLine()`
      # here reads actually length prefixed string.
      var line = await stream.transp.readLine()
      check line == expect
      testFuture.complete(line)
      await stream.close()

    await daemonNode.addHandler(protos, daemonHandler)
    let conn = await nativeNode.dial(NativePeerInfo.init(daemonPeer.peer,
                                                          daemonPeer.addresses),
                                                          protos[0])
    await conn.writeLp(test & "\r\n")
    check expect == (await wait(testFuture, 10.secs))

    await conn.close()
    await nativeNode.stop()
    await allFutures(awaiters)
    await daemonNode.close()

  asyncTest "daemon -> native connection":
    var protos = @["/test-stream"]
    var test = "TEST STRING"

    var testFuture = newFuture[string]("test.future")
    proc nativeHandler(conn: Connection, proto: string) {.async.} =
      var line = string.fromBytes(await conn.readLp(1024))
      check line == test
      testFuture.complete(line)
      await conn.close()

    # custom proto
    var proto = new LPProtocol
    proto.handler = nativeHandler
    proto.codec = protos[0] # codec

    let nativeNode = newStandardSwitch(
      secureManagers = [SecureProtocol.Noise], outTimeout = 5.minutes)

    nativeNode.mount(proto)

    let awaiters = await nativeNode.start()
    let nativePeer = nativeNode.peerInfo

    let daemonNode = await newDaemonApi()
    await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)
    var stream = await daemonNode.openStream(nativePeer.peerId, protos)
    discard await stream.transp.writeLp(test)

    check test == (await wait(testFuture, 10.secs))

    await stream.close()
    await nativeNode.stop()
    await allFutures(awaiters)
    await daemonNode.close()
    await sleepAsync(1.seconds)

  asyncTest "daemon -> multiple reads and writes":
    var protos = @["/test-stream"]

    var testFuture = newFuture[void]("test.future")
    proc nativeHandler(conn: Connection, proto: string) {.async.} =
      check "test 1" == string.fromBytes(await conn.readLp(1024))
      await conn.writeLp("test 2".toBytes())

      check "test 3" == string.fromBytes(await conn.readLp(1024))
      await conn.writeLp("test 4".toBytes())

      testFuture.complete()
      await conn.close()

    # custom proto
    var proto = new LPProtocol
    proto.handler = nativeHandler
    proto.codec = protos[0] # codec

    let nativeNode = newStandardSwitch(
      secureManagers = [SecureProtocol.Noise], outTimeout = 5.minutes)

    nativeNode.mount(proto)

    let awaiters = await nativeNode.start()
    let nativePeer = nativeNode.peerInfo

    let daemonNode = await newDaemonApi()
    await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)
    var stream = await daemonNode.openStream(nativePeer.peerId, protos)

    asyncDiscard stream.transp.writeLp("test 1")
    check "test 2" == string.fromBytes(await stream.transp.readLp())

    asyncDiscard stream.transp.writeLp("test 3")
    check "test 4" == string.fromBytes(await stream.transp.readLp())

    await wait(testFuture, 10.secs)

    await stream.close()
    await nativeNode.stop()
    await allFutures(awaiters)
    await daemonNode.close()

  asyncTest "read write multiple":
    var protos = @["/test-stream"]
    var test = "TEST STRING"

    var count = 0
    var testFuture = newFuture[int]("test.future")
    proc nativeHandler(conn: Connection, proto: string) {.async.} =
      while count < 10:
        var line = string.fromBytes(await conn.readLp(1024))
        check line == test
        await conn.writeLp(test.toBytes())
        count.inc()

      testFuture.complete(count)
      await conn.close()

    # custom proto
    var proto = new LPProtocol
    proto.handler = nativeHandler
    proto.codec = protos[0] # codec

    let nativeNode = newStandardSwitch(
      secureManagers = [SecureProtocol.Noise], outTimeout = 5.minutes)

    nativeNode.mount(proto)

    let awaiters = await nativeNode.start()
    let nativePeer = nativeNode.peerInfo

    let daemonNode = await newDaemonApi()
    await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)
    var stream = await daemonNode.openStream(nativePeer.peerId, protos)

    var count2 = 0
    while count2 < 10:
      discard await stream.transp.writeLp(test)
      let line = await stream.transp.readLp()
      check test == string.fromBytes(line)
      inc(count2)

    check 10 == (await wait(testFuture, 1.minutes))
    await stream.close()
    await nativeNode.stop()
    await allFutures(awaiters)
    await daemonNode.close()

  asyncTest "floodsub: daemon publish one":
    await testPubSubDaemonPublish()

  asyncTest "floodsub: daemon publish many":
    await testPubSubDaemonPublish(count = 10)

  asyncTest "gossipsub: daemon publish one":
    await testPubSubDaemonPublish(gossip = true)

  asyncTest "gossipsub: daemon publish many":
    await testPubSubDaemonPublish(gossip = true, count = 10)

  asyncTest "floodsub: node publish one":
    await testPubSubNodePublish()

  asyncTest "floodsub: node publish many":
    await testPubSubNodePublish(count = 10)

  asyncTest "gossipsub: node publish one":
    await testPubSubNodePublish(gossip = true)

  asyncTest "gossipsub: node publish many":
    await testPubSubNodePublish(gossip = true, count = 10)
