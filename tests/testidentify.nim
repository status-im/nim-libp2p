import unittest, options, bearssl
import chronos, strutils
import ../libp2p/[protocols/identify,
                  multiaddress,
                  peerinfo,
                  peerid,
                  stream/connection,
                  multistream,
                  transports/transport,
                  transports/tcptransport,
                  crypto/crypto]
import ./helpers

when defined(nimHasUsed): {.used.}

suite "Identify":
  teardown:
    checkTrackers()

  suite "handle identify message":
    var
      ma {.threadvar.}: MultiAddress
      remoteSecKey {.threadvar.}: PrivateKey
      remotePeerInfo {.threadvar.}: PeerInfo
      serverFut {.threadvar.}: Future[void]
      acceptFut {.threadvar.}: Future[void]
      identifyProto1 {.threadvar.}: Identify
      identifyProto2 {.threadvar.}: Identify
      transport1 {.threadvar.}: Transport
      transport2 {.threadvar.}: Transport
      msListen {.threadvar.}: MultistreamSelect
      msDial {.threadvar.}: MultistreamSelect
      conn {.threadvar.}: Connection
      exposedAddr {.threadvar.}: MultiAddress

    asyncSetup:
      ma = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      remoteSecKey = PrivateKey.random(ECDSA, rng[]).get()
      remotePeerInfo = PeerInfo.init(
        remoteSecKey, [ma], ["/test/proto1/1.0.0", "/test/proto2/1.0.0"])

      transport1 = TcpTransport.init()
      transport2 = TcpTransport.init()

      exposedAddr = Multiaddress.init("/ip4/192.168.1.1/tcp/1337").get()

      identifyProto1 = newIdentify(remotePeerInfo, proc(): MultiAddress = exposedAddr)
      identifyProto2 = newIdentify(remotePeerInfo)

      msListen = newMultistream()
      msDial = newMultistream()

    asyncTeardown:
      await conn.close()
      await acceptFut
      await transport1.stop()
      await serverFut
      await transport2.stop()

    asyncTest "default agent version":
      msListen.addHandler(IdentifyCodec, identifyProto1)
      serverFut = transport1.start(ma)
      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        await msListen.handle(c)

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      discard await msDial.select(conn, IdentifyCodec)
      let id = await identifyProto2.identify(conn, remotePeerInfo)

      check id.pubKey.get() == remoteSecKey.getKey().get()
      check id.addrs[0] == ma
      check id.protoVersion.get() == ProtoVersion
      check id.agentVersion.get() == AgentVersion
      check id.protos == @["/test/proto1/1.0.0", "/test/proto2/1.0.0"]

    asyncTest "custom agent version":
      const customAgentVersion = "MY CUSTOM AGENT STRING"
      remotePeerInfo.agentVersion = customAgentVersion
      msListen.addHandler(IdentifyCodec, identifyProto1)

      serverFut = transport1.start(ma)

      proc acceptHandler(): Future[void] {.async, gcsafe.} =
        let c = await transport1.accept()
        await msListen.handle(c)

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      discard await msDial.select(conn, IdentifyCodec)
      let id = await identifyProto2.identify(conn, remotePeerInfo)

      check id.pubKey.get() == remoteSecKey.getKey().get()
      check id.addrs[0] == ma
      check id.observedAddr.get() == exposedAddr
      check id.protoVersion.get() == ProtoVersion
      check id.agentVersion.get() == customAgentVersion
      check id.protos == @["/test/proto1/1.0.0", "/test/proto2/1.0.0"]

    asyncTest "handle failed identify":
      msListen.addHandler(IdentifyCodec, identifyProto1)
      asyncCheck transport1.start(ma)

      proc acceptHandler() {.async.} =
        var conn: Connection
        try:
          conn = await transport1.accept()
          await msListen.handle(conn)
        except CatchableError:
          discard
        finally:
          await conn.close()

      acceptFut = acceptHandler()
      conn = await transport2.dial(transport1.ma)

      expect IdentityNoMatchError:
        let pi2 = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
        discard await msDial.select(conn, IdentifyCodec)
        discard await identifyProto2.identify(conn, pi2)

    asyncTest "external address provider":
        msListen.addHandler(IdentifyCodec, identifyProto1)
        serverFut = transport1.start(ma)
        proc acceptHandler(): Future[void] {.async, gcsafe.} =
          let c = await transport1.accept()
          await msListen.handle(c)

        acceptFut = acceptHandler()
        conn = await transport2.dial(transport1.ma)

        discard await msDial.select(conn, IdentifyCodec)
        let id = await identifyProto2.identify(conn, remotePeerInfo)

        check id.pubKey.get() == remoteSecKey.getKey().get()
        check id.addrs[0] == ma
        check id.observedAddr.get() == exposedAddr
        check id.protoVersion.get() == ProtoVersion
        check id.agentVersion.get() == AgentVersion
        check id.protos == @["/test/proto1/1.0.0", "/test/proto2/1.0.0"]
