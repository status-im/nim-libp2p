{.used.}

import options, sequtils
import chronos
import stew/byteutils
import nimcrypto/sysrand
import ../libp2p/[errors,
                  switch,
                  multistream,
                  builders,
                  stream/bufferstream,
                  stream/connection,
                  multiaddress,
                  peerinfo,
                  crypto/crypto,
                  protocols/protocol,
                  protocols/secure/secure,
                  muxers/muxer,
                  muxers/mplex/lpchannel,
                  stream/lpstream,
                  stream/chronosstream,
                  transports/tcptransport]

import ./helpers

const
  TestCodec = "/test/proto/1.0.0"

type
  TestProto = ref object of LPProtocol

suite "Switch":
  teardown:
    checkTrackers()

  asyncTest "e2e use switch dial proto string":
    let done = newFuture[void]()
    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch()
    switch1.mount(testProto)

    let switch2 = newStandardSwitch()
    var awaiters: seq[Future[void]]
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.close()

    await allFuturesThrowing(
      done.wait(5.seconds),
      switch1.stop(),
      switch2.stop())

    # this needs to go at end
    await allFuturesThrowing(awaiters)

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e use switch dial proto string with custom matcher":
    let done = newFuture[void]()
    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let callProto = TestCodec & "/pew"

    proc match(proto: string): bool {.gcsafe.} =
      return proto == callProto

    let switch1 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    switch1.mount(testProto, match)

    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    var awaiters: seq[Future[void]]
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, callProto)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.close()

    await allFuturesThrowing(
      done.wait(5.seconds),
      switch1.stop(),
      switch2.stop())

    # this needs to go at end
    await allFuturesThrowing(awaiters)

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e should not leak bufferstreams and connections on channel close":
    let done = newFuture[void]()
    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
        await conn.writeLp("Hello!")
      finally:
        await conn.close()
        done.complete()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch()
    switch1.mount(testProto)

    let switch2 = newStandardSwitch()
    var awaiters: seq[Future[void]]
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.close()

    await allFuturesThrowing(
      done.wait(5.seconds),
      switch1.stop(),
      switch2.stop(),
    )

    # this needs to go at end
    await allFuturesThrowing(awaiters)

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e use connect then dial":
    var awaiters: seq[Future[void]]

    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        check "Hello!" == msg
      finally:
        await conn.writeLp("Hello!")
        await conn.close()

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    switch1.mount(testProto)

    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)
    let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await conn.writeLp("Hello!")
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg

    await allFuturesThrowing(
      conn.close(),
      switch1.stop(),
      switch2.stop()
    )
    await allFuturesThrowing(awaiters)

    check not switch1.isConnected(switch2.peerInfo.peerId)
    check not switch2.isConnected(switch1.peerInfo.peerId)

  asyncTest "e2e should not leak on peer disconnect":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()
    let switch2 = newStandardSwitch()
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId)

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should trigger connection events (remote)":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()
    let switch2 = newStandardSwitch()

    var step = 0
    var kinds: set[ConnEventKind]
    proc hook(peerId: PeerID, event: ConnEvent) {.async, gcsafe.} =
      kinds = kinds + {event.kind}
      case step:
      of 0:
        check:
          event.kind == ConnEventKind.Connected
          peerId == switch1.peerInfo.peerId
      of 1:
        check:
          event.kind == ConnEventKind.Disconnected

        check peerId == switch1.peerInfo.peerId
      else:
        check false

      step.inc()

    switch2.addConnEventHandler(hook, ConnEventKind.Connected)
    switch2.addConnEventHandler(hook, ConnEventKind.Disconnected)

    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId)

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    check:
      kinds == {
        ConnEventKind.Connected,
        ConnEventKind.Disconnected
      }

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should trigger connection events (local)":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()
    let switch2 = newStandardSwitch()

    var step = 0
    var kinds: set[ConnEventKind]
    proc hook(peerId: PeerID, event: ConnEvent) {.async, gcsafe.} =
      kinds = kinds + {event.kind}
      case step:
      of 0:
        check:
          event.kind == ConnEventKind.Connected
          peerId == switch2.peerInfo.peerId
      of 1:
        check:
          event.kind == ConnEventKind.Disconnected

        check peerId == switch2.peerInfo.peerId
      else:
        check false

      step.inc()

    switch1.addConnEventHandler(hook, ConnEventKind.Connected)
    switch1.addConnEventHandler(hook, ConnEventKind.Disconnected)

    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId)

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    check:
      kinds == {
        ConnEventKind.Connected,
        ConnEventKind.Disconnected
      }

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should trigger peer events (remote)":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()
    let switch2 = newStandardSwitch()

    var step = 0
    var kinds: set[PeerEventKind]
    proc handler(peerId: PeerID, event: PeerEvent) {.async, gcsafe.} =
      kinds = kinds + {event.kind}
      case step:
      of 0:
        check:
          event.kind == PeerEventKind.Joined
          peerId == switch2.peerInfo.peerId
      of 1:
        check:
          event.kind == PeerEventKind.Left
          peerId == switch2.peerInfo.peerId
      else:
        check false

      step.inc()

    switch1.addPeerEventHandler(handler, PeerEventKind.Joined)
    switch1.addPeerEventHandler(handler, PeerEventKind.Left)

    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId)

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    check:
      kinds == {
        PeerEventKind.Joined,
        PeerEventKind.Left
      }

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should trigger peer events (local)":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()
    let switch2 = newStandardSwitch()

    var step = 0
    var kinds: set[PeerEventKind]
    proc handler(peerId: PeerID, event: PeerEvent) {.async, gcsafe.} =
      kinds = kinds + {event.kind}
      case step:
      of 0:
        check:
          event.kind == PeerEventKind.Joined
          peerId == switch1.peerInfo.peerId
      of 1:
        check:
          event.kind == PeerEventKind.Left
          peerId == switch1.peerInfo.peerId
      else:
        check false

      step.inc()

    switch2.addPeerEventHandler(handler, PeerEventKind.Joined)
    switch2.addPeerEventHandler(handler, PeerEventKind.Left)

    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs)

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId)

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    check:
      kinds == {
        PeerEventKind.Joined,
        PeerEventKind.Left
      }

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should trigger peer events only once per peer":
    var awaiters: seq[Future[void]]

    let switch1 = newStandardSwitch()

    let rng = newRng()
    # use same private keys to emulate two connection from same peer
    let privKey = PrivateKey.random(rng[]).tryGet()
    let switch2 = newStandardSwitch(
      privKey = some(privKey),
      rng = rng)

    let switch3 = newStandardSwitch(
      privKey = some(privKey),
      rng = rng)

    var step = 0
    var kinds: set[PeerEventKind]
    proc handler(peerId: PeerID, event: PeerEvent) {.async, gcsafe.} =
      kinds = kinds + {event.kind}
      case step:
      of 0:
        check:
          event.kind == PeerEventKind.Joined
      of 1:
        check:
          event.kind == PeerEventKind.Left
      else:
        check false # should not trigger this

      step.inc()

    switch1.addPeerEventHandler(handler, PeerEventKind.Joined)
    switch1.addPeerEventHandler(handler, PeerEventKind.Left)

    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())
    awaiters.add(await switch3.start())

    await switch2.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs) # should trigger 1st Join event
    await switch3.connect(switch1.peerInfo.peerId, switch1.peerInfo.addrs) # should trigger 2nd Join event

    check switch1.isConnected(switch2.peerInfo.peerId)
    check switch2.isConnected(switch1.peerInfo.peerId)
    check switch3.isConnected(switch1.peerInfo.peerId)

    await switch2.disconnect(switch1.peerInfo.peerId) # should trigger 1st Left event
    await switch3.disconnect(switch1.peerInfo.peerId) # should trigger 2nd Left event

    check not switch2.isConnected(switch1.peerInfo.peerId)
    check not switch3.isConnected(switch1.peerInfo.peerId)
    check await(checkExpiring((not switch1.isConnected(switch2.peerInfo.peerId))))
    check await(checkExpiring((not switch1.isConnected(switch3.peerInfo.peerId))))

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    check:
      kinds == {
        PeerEventKind.Joined,
        PeerEventKind.Left
      }

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop(),
      switch3.stop())
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should allow dropping peer from connection events":
    var awaiters: seq[Future[void]]

    let rng = newRng()
    # use same private keys to emulate two connection from same peer
    let peerInfo = PeerInfo.init(PrivateKey.random(rng[]).tryGet())

    var switches: seq[Switch]
    var done = newFuture[void]()
    var onConnect: Future[void]
    proc hook(peerId: PeerID, event: ConnEvent) {.async, gcsafe.} =
      case event.kind:
      of ConnEventKind.Connected:
        await onConnect
        await switches[0].disconnect(peerInfo.peerId) # trigger disconnect
      of ConnEventKind.Disconnected:
        check not switches[0].isConnected(peerInfo.peerId)
        await sleepAsync(1.millis)
        done.complete()

    switches.add(newStandardSwitch(
        rng = rng))

    switches[0].addConnEventHandler(hook, ConnEventKind.Connected)
    switches[0].addConnEventHandler(hook, ConnEventKind.Disconnected)
    awaiters.add(await switches[0].start())

    switches.add(newStandardSwitch(
      privKey = some(peerInfo.privateKey),
      rng = rng))
    onConnect = switches[1].connect(switches[0].peerInfo.peerId, switches[0].peerInfo.addrs)
    await onConnect

    await done
    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    await allFuturesThrowing(
      switches.mapIt( it.stop() ))
    await allFuturesThrowing(awaiters)

  asyncTest "e2e should allow dropping multiple connections for peer from connection events":
    var awaiters: seq[Future[void]]

    let rng = newRng()
    # use same private keys to emulate two connection from same peer
    let peerInfo = PeerInfo.init(PrivateKey.random(rng[]).tryGet())

    var conns = 1
    var switches: seq[Switch]
    var done = newFuture[void]()
    var onConnect: Future[void]
    proc hook(peerId: PeerID, event: ConnEvent) {.async, gcsafe.} =
      case event.kind:
      of ConnEventKind.Connected:
        if conns == 5:
          await onConnect
          await switches[0].disconnect(peerInfo.peerId) # trigger disconnect
          return

        conns.inc
      of ConnEventKind.Disconnected:
        if conns == 1:
          check not switches[0].isConnected(peerInfo.peerId)
          done.complete()
        conns.dec

    switches.add(newStandardSwitch(
        rng = rng))

    switches[0].addConnEventHandler(hook, ConnEventKind.Connected)
    switches[0].addConnEventHandler(hook, ConnEventKind.Disconnected)
    awaiters.add(await switches[0].start())

    for i in 1..5:
      switches.add(newStandardSwitch(
        privKey = some(peerInfo.privateKey),
        rng = rng))
      onConnect = switches[i].connect(switches[0].peerInfo.peerId, switches[0].peerInfo.addrs)
      await onConnect

    await done
    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)

    await allFuturesThrowing(
      switches.mapIt( it.stop() ))
    await allFuturesThrowing(awaiters)

  # TODO: we should be able to test cancellation
  # for most of the steps in the upgrade flow -
  # this is just a basic test for dials
  asyncTest "e2e canceling dial should not leak":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    let transport = TcpTransport.init(upgrade = Upgrade())
    await transport.start(ma)

    proc acceptHandler() {.async, gcsafe.} =
      try:
        let conn = await transport.accept()
        discard await conn.readLp(100)
      except CatchableError:
        discard

    let handlerWait = acceptHandler()
    let switch = newStandardSwitch(secureManagers = [SecureProtocol.Noise])

    var awaiters: seq[Future[void]]
    awaiters.add(await switch.start())

    var peerId = PeerID.init(PrivateKey.random(ECDSA, rng[]).get()).get()
    let connectFut = switch.connect(peerId, @[transport.ma])
    await sleepAsync(500.millis)
    connectFut.cancel()
    await handlerWait

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)
    checkTracker(ChronosStreamTrackerName)

    await allFuturesThrowing(
      transport.stop(),
      switch.stop())

    # this needs to go at end
    await allFuturesThrowing(awaiters)

  asyncTest "e2e closing remote conn should not leak":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    let transport = TcpTransport.init(upgrade = Upgrade())
    await transport.start(ma)

    proc acceptHandler() {.async, gcsafe.} =
      let conn = await transport.accept()
      await conn.closeWithEOF()

    let handlerWait = acceptHandler()
    let switch = newStandardSwitch(secureManagers = [SecureProtocol.Noise])

    var awaiters: seq[Future[void]]
    awaiters.add(await switch.start())

    var peerId = PeerID.init(PrivateKey.random(ECDSA, rng[]).get()).get()
    expect LPStreamClosedError:
      await switch.connect(peerId, @[transport.ma])

    await handlerWait

    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)
    checkTracker(ChronosStreamTrackerName)

    await allFuturesThrowing(
      transport.stop(),
      switch.stop())

    # this needs to go at end
    await allFuturesThrowing(awaiters)

  asyncTest "e2e calling closeWithEOF on the same stream should not assert":
    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      discard await conn.readLp(100)

    let testProto = new TestProto
    testProto.codec = TestCodec
    testProto.handler = handle

    let switch1 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    switch1.mount(testProto)

    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])

    var awaiters: seq[Future[void]]
    awaiters.add(await switch1.start())

    let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

    proc closeReader() {.async.} =
      await conn.closeWithEOF()

    var readers: seq[Future[void]]
    for i in 0..10:
      readers.add(closeReader())

    await allFuturesThrowing(readers)
    checkTracker(LPChannelTrackerName)
    checkTracker(SecureConnTrackerName)
    checkTracker(ChronosStreamTrackerName)

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop())

    # this needs to go at end
    await allFuturesThrowing(awaiters)

  asyncTest "connect to inexistent peer":
    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    discard await switch2.start()
    let someAddr = MultiAddress.init("/ip4/127.128.0.99").get()
    let seckey = PrivateKey.random(ECDSA, rng[]).get()
    let somePeer = PeerInfo.init(secKey, [someAddr])
    expect(DialFailedError):
      discard await switch2.dial(somePeer.peerId, somePeer.addrs, TestCodec)
    await switch2.stop()

  asyncTest "e2e total connection limits on incoming connections":
    var awaiters: seq[Future[void]]

    var switches: seq[Switch]
    let destSwitch = newStandardSwitch(maxConnections = 3)
    switches.add(destSwitch)
    awaiters.add(await destSwitch.start())

    let destPeerInfo = destSwitch.peerInfo
    for i in 0..<3:
      let switch = newStandardSwitch()
      switches.add(switch)
      awaiters.add(await switch.start())

      check await switch.connect(destPeerInfo.peerId, destPeerInfo.addrs)
        .withTimeout(1000.millis)

    let switchFail = newStandardSwitch()
    switches.add(switchFail)
    awaiters.add(await switchFail.start())

    check not(await switchFail.connect(destPeerInfo.peerId, destPeerInfo.addrs)
      .withTimeout(1000.millis))

    await allFuturesThrowing(
      allFutures(switches.mapIt( it.stop() )))
    await allFuturesThrowing(awaiters)

  asyncTest "e2e total connection limits on incoming connections":
    var awaiters: seq[Future[void]]

    var switches: seq[Switch]
    for i in 0..<3:
      switches.add(newStandardSwitch())
      awaiters.add(await switches[i].start())

    let srcSwitch = newStandardSwitch(maxConnections = 3)
    awaiters.add(await srcSwitch.start())

    let dstSwitch = newStandardSwitch()
    awaiters.add(await dstSwitch.start())

    for s in switches:
      check await srcSwitch.connect(s.peerInfo.peerId, s.peerInfo.addrs)
      .withTimeout(1000.millis)

    expect TooManyConnectionsError:
      await srcSwitch.connect(dstSwitch.peerInfo.peerId, dstSwitch.peerInfo.addrs)

    switches.add(srcSwitch)
    switches.add(dstSwitch)

    await allFuturesThrowing(
      allFutures(switches.mapIt( it.stop() )))
    await allFuturesThrowing(awaiters)

  asyncTest "e2e max incoming connection limits":
    var awaiters: seq[Future[void]]

    var switches: seq[Switch]
    let destSwitch = newStandardSwitch(maxIn = 3)
    switches.add(destSwitch)
    awaiters.add(await destSwitch.start())

    let destPeerInfo = destSwitch.peerInfo
    for i in 0..<3:
      let switch = newStandardSwitch()
      switches.add(switch)
      awaiters.add(await switch.start())

      check await switch.connect(destPeerInfo.peerId, destPeerInfo.addrs)
        .withTimeout(1000.millis)

    let switchFail = newStandardSwitch()
    switches.add(switchFail)
    awaiters.add(await switchFail.start())

    check not(await switchFail.connect(destPeerInfo.peerId, destPeerInfo.addrs)
      .withTimeout(1000.millis))

    await allFuturesThrowing(
      allFutures(switches.mapIt( it.stop() )))
    await allFuturesThrowing(awaiters)

  asyncTest "e2e max outgoing connection limits":
    var awaiters: seq[Future[void]]

    var switches: seq[Switch]
    for i in 0..<3:
      switches.add(newStandardSwitch())
      awaiters.add(await switches[i].start())

    let srcSwitch = newStandardSwitch(maxOut = 3)
    awaiters.add(await srcSwitch.start())

    let dstSwitch = newStandardSwitch()
    awaiters.add(await dstSwitch.start())

    for s in switches:
      check await srcSwitch.connect(s.peerInfo.peerId, s.peerInfo.addrs)
      .withTimeout(1000.millis)

    expect TooManyConnectionsError:
      await srcSwitch.connect(dstSwitch.peerInfo.peerId, dstSwitch.peerInfo.addrs)

    switches.add(srcSwitch)
    switches.add(dstSwitch)

    await allFuturesThrowing(
      allFutures(switches.mapIt( it.stop() )))
    await allFuturesThrowing(awaiters)
