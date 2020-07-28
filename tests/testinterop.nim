import options, tables
import unittest
import chronos, chronicles, stew/byteutils
import helpers
import ../libp2p/[daemon/daemonapi,
                  vbuffer,
                  multiaddress,
                  multicodec,
                  cid,
                  varint,
                  multihash,
                  standard_setup,
                  peerid,
                  peerinfo,
                  switch,
                  stream/connection,
                  muxers/muxer,
                  crypto/crypto,
                  muxers/mplex/mplex,
                  muxers/muxer,
                  muxers/mplex/types,
                  protocols/protocol,
                  protocols/identify,
                  transports/transport,
                  transports/tcptransport,
                  protocols/secure/secure,
                  protocols/pubsub/pubsub,
                  protocols/pubsub/gossipsub,
                  protocols/pubsub/floodsub]

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

proc testPubSubDaemonPublish(gossip: bool = false,
                             count: int = 1): Future[bool] {.async.} =
  var pubsubData = "TEST MESSAGE"
  var testTopic = "test-topic"
  var msgData = pubsubData.toBytes()

  var flags = {PSFloodSub}
  if gossip:
    flags = {PSGossipSub}

  let daemonNode = await newDaemonApi(flags)
  let daemonPeer = await daemonNode.identity()
  let nativeNode = newStandardSwitch(
    gossip = gossip,
    secureManagers = [SecureProtocol.Noise],
    outTimeout = 5.minutes)

  let awaiters = nativeNode.start()
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
  await nativeNode.connect(peer)
  let subscribeHanle = nativeNode.subscribePeer(peer)

  await sleepAsync(1.seconds)
  await daemonNode.connect(nativePeer.peerId, nativePeer.addrs)

  proc pubsubHandler(api: DaemonAPI,
                     ticket: PubsubTicket,
                     message: PubSubMessage): Future[bool] {.async.} =
    result = true # don't cancel subscription

  asyncDiscard daemonNode.pubsubSubscribe(testTopic, pubsubHandler)
  await nativeNode.subscribe(testTopic, nativeHandler)
  await sleepAsync(5.seconds)

  proc publisher() {.async.} =
    while not finished:
      await daemonNode.pubsubPublish(testTopic, msgData)
      await sleepAsync(500.millis)

  await wait(publisher(), 5.minutes) # should be plenty of time

  result = true
  await nativeNode.stop()
  await allFutures(awaiters)
  await daemonNode.close()
  await subscribeHanle

proc testPubSubNodePublish(gossip: bool = false,
                           count: int = 1): Future[bool] {.async.} =
  var pubsubData = "TEST MESSAGE"
  var testTopic = "test-topic"
  var msgData = pubsubData.toBytes()

  var flags = {PSFloodSub}
  if gossip:
    flags = {PSGossipSub}

  let daemonNode = await newDaemonApi(flags)
  let daemonPeer = await daemonNode.identity()
  let nativeNode = newStandardSwitch(
    gossip = gossip,
    secureManagers = [SecureProtocol.Secio],
    outTimeout = 5.minutes)

  let awaiters = nativeNode.start()
  let nativePeer = nativeNode.peerInfo

  let peer = NativePeerInfo.init(
    daemonPeer.peer,
    daemonPeer.addresses)
  await nativeNode.connect(peer)
  let subscribeHandle = nativeNode.subscribePeer(peer)

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
  await nativeNode.subscribe(testTopic, nativeHandler)
  await sleepAsync(5.seconds)

  proc publisher() {.async.} =
    while not finished:
      discard await nativeNode.publish(testTopic, msgData)
      await sleepAsync(500.millis)

  await wait(publisher(), 5.minutes) # should be plenty of time

  result = finished
  await nativeNode.stop()
  await allFutures(awaiters)
  await daemonNode.close()
  await subscribeHandle

suite "Interop":
  # TODO: chronos transports are leaking,
  # but those are tracked for both the daemon
  # and libp2p, so not sure which one it is,
  # need to investigate more
  # teardown:
  #   for tracker in testTrackers():
  #     # echo tracker.dump()
  #     # check tracker.isLeaked() == false

  test "native -> daemon multiple reads and writes":
    proc runTests(): Future[bool] {.async.} =
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
        asyncDiscard stream.transp.writeLp("test 2")

        await sleepAsync(10.millis)
        check string.fromBytes(await stream.transp.readLp()) == "test 3"
        asyncDiscard stream.transp.writeLp("test 4")
        testFuture.complete()
        await stream.close()

      await daemonNode.addHandler(protos, daemonHandler)
      let conn = await nativeNode.dial(NativePeerInfo.init(daemonPeer.peer,
                                                           daemonPeer.addresses),
                                                           protos[0])
      await conn.writeLp("test 1")
      check "test 2" == string.fromBytes((await conn.readLp(1024)))
      await sleepAsync(10.millis)

      await conn.writeLp("test 3")
      check "test 4" == string.fromBytes((await conn.readLp(1024)))

      await wait(testFuture, 10.secs)
      await conn.close()

      await daemonNode.close()
      await nativeNode.stop()
      await allFutures(awaiters)

      await sleepAsync(1.seconds)
      result = true

    check:
      waitFor(runTests()) == true

  test "native -> daemon connection":
    proc runTests(): Future[bool] {.async.} =
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
        secureManagers = [SecureProtocol.Secio],
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
      result = expect == (await wait(testFuture, 10.secs))

      await conn.close()
      await nativeNode.stop()
      await allFutures(awaiters)
      await daemonNode.close()

    check:
      waitFor(runTests()) == true

  test "daemon -> native connection":
    proc runTests(): Future[bool] {.async.} =
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

      result = test == (await wait(testFuture, 10.secs))

      await stream.close()
      await nativeNode.stop()
      await allFutures(awaiters)
      await daemonNode.close()
      await sleepAsync(1.seconds)

    check:
      waitFor(runTests()) == true

  test "daemon -> multiple reads and writes":
    proc runTests(): Future[bool] {.async.} =
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
        secureManagers = [SecureProtocol.Secio], outTimeout = 5.minutes)

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

      result = true
      await stream.close()
      await nativeNode.stop()
      await allFutures(awaiters)
      await daemonNode.close()

    check:
      waitFor(runTests()) == true

  test "read write multiple":
    proc runTests(): Future[bool] {.async.} =
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

      result = 10 == (await wait(testFuture, 1.minutes))
      await stream.close()
      await nativeNode.stop()
      await allFutures(awaiters)
      await daemonNode.close()

    check:
      waitFor(runTests()) == true

  test "floodsub: daemon publish one":
    check:
      waitFor(testPubSubDaemonPublish()) == true

  test "floodsub: daemon publish many":
    check:
      waitFor(testPubSubDaemonPublish(count = 10)) == true

  test "gossipsub: daemon publish one":
    check:
      waitFor(testPubSubDaemonPublish(gossip = true)) == true

  test "gossipsub: daemon publish many":
    check:
      waitFor(testPubSubDaemonPublish(gossip = true, count = 10)) == true

  test "floodsub: node publish one":
    check:
      waitFor(testPubSubNodePublish()) == true

  test "floodsub: node publish many":
    check:
      waitFor(testPubSubNodePublish(count = 10)) == true

  test "gossipsub: node publish one":
    check:
      waitFor(testPubSubNodePublish(gossip = true)) == true

  test "gossipsub: node publish many":
    check:
      waitFor(testPubSubNodePublish(gossip = true, count = 10)) == true
