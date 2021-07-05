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
      secondaryTracker {.threadvar.}: P2PSecondaryTracker
      transport2 {.threadvar.}: Transport
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

      pingReceivedCount = 0

      serverFut = transport1.start(ma)

      secondaryTracker = getSecondaryTracker()

    asyncTeardown:
      await conn.close()
      await acceptFut

      secondaryTracker.check()
      await transport1.stop()
      await serverFut
      await transport2.stop()

    asyncTest "simple ping":
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        await pingProto1.handler(c, "ping")

      acceptFut = acceptHandler()

      conn = await transport2.dial(transport1.ma)

      let time = await pingProto2.ping(conn)

      check not time.isZero()

    asyncTest "ping callback":
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        discard await pingProto1.ping(c)
        await c.close()

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      await pingProto2.handler(conn, "ping")
      check pingReceivedCount == 1

    asyncTest "bad ping data ack":
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let conn = await transport1.accept()
        var
          buf: array[32, byte]
          fakebuf: array[32, byte]
        await conn.readExactly(addr buf[0], 32)
        await conn.write(addr fakebuf[0], 32)
        await conn.close()

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      let p = pingProto2.ping(conn)
      var raised = false
      try:
        discard await p
        check false #should have raised
      except WrongPingAckError:
        raised = true
      check raised
