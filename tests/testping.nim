import options, bearssl
import chronos, strutils
import ../libp2p/[protocols/identify,
                  protocols/ping,
                  multiaddress,
                  peerinfo,
                  wire,
                  peerid,
                  stream/connection,
                  multistream,
                  transports/transport,
                  transports/tcptransport,
                  crypto/crypto,
                  upgrademngrs/upgrade]
import ./helpers

when defined(nimHasUsed): {.used.}

suite "Ping":
  teardown:
    checkTrackers()

  suite "handle ping message":
    var
      ma {.threadvar.}: MultiAddress
      serverFut {.threadvar.}: Future[void]
      acceptFut {.threadvar.}: Future[void]
      pingProto1 {.threadvar.}: Ping
      pingProto2 {.threadvar.}: Ping
      transport1 {.threadvar.}: Transport
      transport2 {.threadvar.}: Transport
      msListen {.threadvar.}: MultistreamSelect
      msDial {.threadvar.}: MultistreamSelect
      conn {.threadvar.}: Connection
      pingReceivedCount {.threadvar.}: int

    asyncSetup:
      ma = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      transport1 = TcpTransport.init(upgrade = Upgrade())
      transport2 = TcpTransport.init(upgrade = Upgrade())

      proc handlePing(peer: PeerInfo) {.async, gcsafe, closure.} =
        inc pingReceivedCount
      pingProto1 = Ping.new()
      pingProto2 = Ping.new(handlePing)

      msListen = newMultistream()
      msDial = newMultistream()

      pingReceivedCount = 0

    asyncTeardown:
      await conn.close()
      await acceptFut
      await transport1.stop()
      await serverFut
      await transport2.stop()

    asyncTest "simple ping":
      msListen.addHandler(PingCodec, pingProto1)
      serverFut = transport1.start(ma)
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        await msListen.handle(c)
        await c.close()

      acceptFut = acceptHandler()

      let tracker = getSecondaryTracker()

      conn = await transport2.dial(transport1.ma)

      discard await msDial.select(conn, PingCodec)
      let time = await pingProto2.ping(conn)

      check not time.isZero()

      await acceptFut
      await sleepAsync(2.seconds)

      checkSecondaryTracker(tracker)

    asyncTest "networked cancel ping":
      proc testPing(): Future[void] {.async.} =
        let baseMa = Multiaddress.init("/ip4/127.0.0.1/tcp/0").tryGet()

        let transport: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        let transportdialer: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        asyncSpawn transport.start(baseMa)

        proc acceptHandler() {.async, gcsafe.} =
          let handler = Ping.new().handler
          let conn = await transport.accept()
          await handler(conn, "na")

        let handlerWait = acceptHandler()

        let streamTransport = await transportdialer.dial(transport.ma)

        discard await pingProto2.ping(streamTransport)

      for pollCount in 0..20:
        #echo "Polling ", pollCount, " times"
        let p = testPing()
        for _ in 0..<pollCount:
          if p.finished: break
          poll()
        if p.finished: break #We actually finished the sequence
        await p.cancelAndWait()
        check p.cancelled

    asyncTest "ping callback":
      msDial.addHandler(PingCodec, pingProto2)
      serverFut = transport1.start(ma)
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        discard await msListen.select(c, PingCodec)
        discard await pingProto1.ping(c)

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      await msDial.handle(conn)
      check pingReceivedCount == 1

    asyncTest "bad ping data ack":
      type FakePing = ref object of LPProtocol
      let fakePingProto = FakePing()
      proc fakeHandle(conn: Connection, proto: string) {.async, gcsafe, closure.} =
        var
          buf: array[32, byte]
          fakebuf: array[32, byte]
        await conn.readExactly(addr buf[0], 32)
        await conn.write(addr fakebuf[0], 32)
      fakePingProto.codec = PingCodec
      fakePingProto.handler = fakeHandle

      msListen.addHandler(PingCodec, fakePingProto)
      serverFut = transport1.start(ma)
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        await msListen.handle(c)

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      discard await msDial.select(conn, PingCodec)
      let p = pingProto2.ping(conn)
      var raised = false
      try:
        discard await p
        check false #should have raised
      except WrongPingAckError:
        raised = true
      check raised
