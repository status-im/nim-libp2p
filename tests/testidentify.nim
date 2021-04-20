import options, bearssl
import chronos, strutils, sequtils, sets, algorithm
import ../libp2p/[protocols/identify,
                  multiaddress,
                  peerinfo,
                  peerid,
                  stream/connection,
                  multistream,
                  transports/transport,
                  switch,
                  builders,
                  transports/tcptransport,
                  crypto/crypto,
                  upgrademngrs/upgrade]
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

    asyncSetup:
      ma = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      remoteSecKey = PrivateKey.random(ECDSA, rng[]).get()
      remotePeerInfo = PeerInfo.init(
        remoteSecKey, [ma], ["/test/proto1/1.0.0", "/test/proto2/1.0.0"])

      transport1 = TcpTransport.init(upgrade = Upgrade())
      transport2 = TcpTransport.init(upgrade = Upgrade())

      identifyProto1 = Identify.new(remotePeerInfo)
      identifyProto2 = Identify.new(remotePeerInfo)

      msListen = MultistreamSelect.new()
      msDial = MultistreamSelect.new()

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
        let c = await transport1.acceptStream()
        await msListen.handle(c)

      acceptFut = acceptHandler()
      conn = await transport2.dialStream(transport1.ma)

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
        let c = await transport1.acceptStream()
        await msListen.handle(c)

      acceptFut = acceptHandler()
      conn = await transport2.dialStream(transport1.ma)

      discard await msDial.select(conn, IdentifyCodec)
      let id = await identifyProto2.identify(conn, remotePeerInfo)

      check id.pubKey.get() == remoteSecKey.getKey().get()
      check id.addrs[0] == ma
      check id.protoVersion.get() == ProtoVersion
      check id.agentVersion.get() == customAgentVersion
      check id.protos == @["/test/proto1/1.0.0", "/test/proto2/1.0.0"]

    asyncTest "handle failed identify":
      msListen.addHandler(IdentifyCodec, identifyProto1)
      asyncSpawn transport1.start(ma)

      proc acceptHandler() {.async.} =
        var conn: Connection
        try:
          conn = await transport1.acceptStream()
          await msListen.handle(conn)
        except CatchableError:
          discard
        finally:
          await conn.close()

      acceptFut = acceptHandler()
      conn = await transport2.dialStream(transport1.ma)

      expect IdentityNoMatchError:
        let pi2 = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
        discard await msDial.select(conn, IdentifyCodec)
        discard await identifyProto2.identify(conn, pi2)

  suite "handle push identify message":
    var
      switch1 {.threadvar.}: Switch
      switch2 {.threadvar.}: Switch
      identifyPush1 {.threadvar.}: IdentifyPush
      identifyPush2 {.threadvar.}: IdentifyPush
      awaiters {.threadvar.}: seq[Future[void]]
      conn {.threadvar.}: Connection
    asyncSetup:
      switch1 = newStandardSwitch()
      switch2 = newStandardSwitch()

      identifyPush1 = IdentifyPush.new(switch1.connManager)
      identifyPush2 = IdentifyPush.new(switch2.connManager)

      switch1.mount(identifyPush1)
      switch2.mount(identifyPush2)

      awaiters.add(await switch1.start())
      awaiters.add(await switch2.start())

      conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, IdentifyPushCodec)

      let storedInfo1 = switch1.peerStore.get(switch2.peerInfo.peerId)
      let storedInfo2 = switch2.peerStore.get(switch1.peerInfo.peerId)

      check:
        storedInfo1.peerId == switch2.peerInfo.peerId
        storedInfo2.peerId == switch1.peerInfo.peerId

        storedInfo1.addrs.toSeq() == switch2.peerInfo.addrs
        storedInfo2.addrs.toSeq() == switch1.peerInfo.addrs

        storedInfo1.protos.toSeq() == switch2.peerInfo.protocols
        storedInfo2.protos.toSeq() == switch1.peerInfo.protocols

    proc closeAll() {.async.} =
      await conn.close()

      await switch1.stop()
      await switch2.stop()

      # this needs to go at end
      await allFuturesThrowing(awaiters)

    asyncTest "simple push identify":
      switch2.peerInfo.protocols.add("/newprotocol/")
      switch2.peerInfo.addrs.add(MultiAddress.init("/ip4/127.0.0.1/tcp/5555").tryGet())

      check:
        switch1.peerStore.get(switch2.peerInfo.peerId).addrs.toSeq() != switch2.peerInfo.addrs
        switch1.peerStore.get(switch2.peerInfo.peerId).protos != switch2.peerInfo.protocols.toSet()

      await identifyPush2.push(switch2.peerInfo, conn)

      await closeAll()

      # Wait the very end to be sure that the push has been processed
      var aprotos = switch1.peerStore.get(switch2.peerInfo.peerId).protos.toSeq()
      var bprotos = switch2.peerInfo.protocols
      aprotos.sort()
      bprotos.sort()
      check:
        aprotos == bprotos
        switch1.peerStore.get(switch2.peerInfo.peerId).addrs == switch2.peerInfo.addrs.toSet()


    asyncTest "wrong peer id push identify":
      switch2.peerInfo.protocols.add("/newprotocol/")
      switch2.peerInfo.addrs.add(MultiAddress.init("/ip4/127.0.0.1/tcp/5555").tryGet())

      check:
        switch1.peerStore.get(switch2.peerInfo.peerId).addrs != switch2.peerInfo.addrs.toSet()
        switch1.peerStore.get(switch2.peerInfo.peerId).protos.toSeq() != switch2.peerInfo.protocols

      let oldPeerId = switch2.peerInfo.peerId
      switch2.peerInfo = PeerInfo.init(PrivateKey.random(newRng()[]).get())

      await identifyPush2.push(switch2.peerInfo, conn)

      await closeAll()

      # Wait the very end to be sure that the push has been processed
      var aprotos = switch1.peerStore.get(oldPeerId).protos.toSeq()
      var bprotos = switch2.peerInfo.protocols
      aprotos.sort()
      bprotos.sort()
      check:
        aprotos != bprotos
        switch1.peerStore.get(oldPeerId).addrs.toSeq() != switch2.peerInfo.addrs
