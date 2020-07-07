## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.used.}

import unittest, tables, bearssl
import chronos, stew/byteutils
import chronicles
import ../libp2p/crypto/crypto
import ../libp2p/[switch,
                  errors,
                  multistream,
                  stream/bufferstream,
                  protocols/identify,
                  stream/connection,
                  transports/transport,
                  transports/tcptransport,
                  multiaddress,
                  peerinfo,
                  crypto/crypto,
                  protocols/protocol,
                  muxers/muxer,
                  muxers/mplex/mplex,
                  muxers/mplex/types,
                  protocols/secure/noise,
                  protocols/secure/secure]
import ./helpers

const
  TestCodec = "/test/proto/1.0.0"

type
  TestProto = ref object of LPProtocol

method init(p: TestProto) {.gcsafe.} =
  proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
    let msg = string.fromBytes(await conn.readLp(1024))
    check "Hello!" == msg
    await conn.writeLp("Hello!")
    await conn.close()

  p.codec = TestCodec
  p.handler = handle

proc createSwitch(ma: MultiAddress; outgoing: bool): (Switch, PeerInfo) =
  var peerInfo: PeerInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get())
  peerInfo.addrs.add(ma)
  let identify = newIdentify(peerInfo)

  proc createMplex(conn: Connection): Muxer =
    result = newMplex(conn)

  let mplexProvider = newMuxerProvider(createMplex, MplexCodec)
  let transports = @[Transport(TcpTransport.init())]
  let muxers = [(MplexCodec, mplexProvider)].toTable()
  let secureManagers = [Secure(newNoise(rng, peerInfo.privateKey, outgoing = outgoing))]
  let switch = newSwitch(peerInfo,
                         transports,
                         identify,
                         muxers,
                         secureManagers)
  result = (switch, peerInfo)

suite "Noise":
  teardown:
    for tracker in testTrackers():
      # echo tracker.dump()
      check tracker.isLeaked() == false

  test "e2e: handle write + noise":
    proc testListenerDialer(): Future[bool] {.async.} =
      let
        server = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
        serverInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [server])
        serverNoise = newNoise(rng, serverInfo.privateKey, outgoing = false)

      proc connHandler(conn: Connection) {.async, gcsafe.} =
        let sconn = await serverNoise.secure(conn, false)
        try:
          await sconn.write("Hello!")
        finally:
          await sconn.close()
          await conn.close()

      let
        transport1: TcpTransport = TcpTransport.init()
      asyncCheck await transport1.listen(server, connHandler)

      let
        transport2: TcpTransport = TcpTransport.init()
        clientInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [transport1.ma])
        clientNoise = newNoise(rng, clientInfo.privateKey, outgoing = true)
        conn = await transport2.dial(transport1.ma)
        sconn = await clientNoise.secure(conn, true)

      var msg = newSeq[byte](6)
      await sconn.readExactly(addr msg[0], 6)

      await sconn.close()
      await conn.close()
      await transport1.close()
      await transport2.close()

      result = string.fromBytes(msg) == "Hello!"

    check:
      waitFor(testListenerDialer()) == true

  test "e2e: handle read + noise":
    proc testListenerDialer(): Future[bool] {.async.} =
      let
        server = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
        serverInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [server])
        serverNoise = newNoise(rng, serverInfo.privateKey, outgoing = false)
        readTask = newFuture[void]()

      proc connHandler(conn: Connection) {.async, gcsafe.} =
        let sconn = await serverNoise.secure(conn, false)
        defer:
          await sconn.close()
          await conn.close()
        var msg = newSeq[byte](6)
        await sconn.readExactly(addr msg[0], 6)
        check string.fromBytes(msg) == "Hello!"
        readTask.complete()

      let
        transport1: TcpTransport = TcpTransport.init()
      asyncCheck await transport1.listen(server, connHandler)

      let
        transport2: TcpTransport = TcpTransport.init()
        clientInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [transport1.ma])
        clientNoise = newNoise(rng, clientInfo.privateKey, outgoing = true)
        conn = await transport2.dial(transport1.ma)
        sconn = await clientNoise.secure(conn, true)

      await sconn.write("Hello!")
      await readTask
      await sconn.close()
      await conn.close()
      await transport1.close()
      await transport2.close()

      result = true

    check:
      waitFor(testListenerDialer()) == true

  test "e2e: handle read + noise fragmented":
    proc testListenerDialer(): Future[bool] {.async.} =
      let
        server = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
        serverInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [server])
        serverNoise = newNoise(rng, serverInfo.privateKey, outgoing = false)
        readTask = newFuture[void]()

      var hugePayload = newSeq[byte](0xFFFFF)
      brHmacDrbgGenerate(rng[], hugePayload)
      trace "Sending huge payload", size = hugePayload.len

      proc connHandler(conn: Connection) {.async, gcsafe.} =
        let sconn = await serverNoise.secure(conn, false)
        defer:
          await sconn.close()
        let msg = await sconn.readLp(1024*1024)
        check msg == hugePayload
        readTask.complete()

      let
        transport1: TcpTransport = TcpTransport.init()
        listenFut = await transport1.listen(server, connHandler)

      let
        transport2: TcpTransport = TcpTransport.init()
        clientInfo = PeerInfo.init(PrivateKey.random(ECDSA, rng[]).get(), [transport1.ma])
        clientNoise = newNoise(rng, clientInfo.privateKey, outgoing = true)
        conn = await transport2.dial(transport1.ma)
        sconn = await clientNoise.secure(conn, true)

      await sconn.writeLp(hugePayload)
      await readTask

      await sconn.close()
      await conn.close()
      await transport2.close()
      await transport1.close()
      await listenFut

      result = true

    check:
      waitFor(testListenerDialer()) == true

  test "e2e use switch dial proto string":
    proc testSwitch(): Future[bool] {.async, gcsafe.} =
      let ma1: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let ma2: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      var peerInfo1, peerInfo2: PeerInfo
      var switch1, switch2: Switch
      var awaiters: seq[Future[void]]

      (switch1, peerInfo1) = createSwitch(ma1, false)

      let testProto = new TestProto
      testProto.init()
      testProto.codec = TestCodec
      switch1.mount(testProto)
      (switch2, peerInfo2) = createSwitch(ma2, true)
      awaiters.add(await switch1.start())
      awaiters.add(await switch2.start())
      let conn = await switch2.dial(switch1.peerInfo, TestCodec)
      await conn.writeLp("Hello!")
      let msg = string.fromBytes(await conn.readLp(1024))
      check "Hello!" == msg
      await conn.close()

      await allFuturesThrowing(
        switch1.stop(),
        switch2.stop())
      await allFuturesThrowing(awaiters)
      result = true

    check:
      waitFor(testSwitch()) == true

  # test "interop with rust noise":
  #   when true: # disable cos in CI we got no interop server/client
  #     proc testListenerDialer(): Future[bool] {.async.} =
  #       const
  #         proto = "/noise/xx/25519/chachapoly/sha256/0.1.0"

  #       let
  #         local = Multiaddress.init("/ip4/0.0.0.0/tcp/23456")
  #         info = PeerInfo.init(PrivateKey.random(ECDSA), [local])
  #         noise = newNoise(info.privateKey)
  #         ms = newMultistream()
  #         transport = TcpTransport.newTransport()

  #       proc connHandler(conn: Connection) {.async, gcsafe.} =
  #         try:
  #           await ms.handle(conn)
  #           trace "ms.handle exited"
  #         except:
  #           error getCurrentExceptionMsg()
  #         finally:
  #           await conn.close()

  #       ms.addHandler(proto, noise)

  #       let
  #         clientConn = await transport.listen(local, connHandler)
  #       await clientConn

  #       result = true

  #     check:
  #       waitFor(testListenerDialer()) == true

  # test "interop with rust noise":
  #   when true: # disable cos in CI we got no interop server/client
  #     proc testListenerDialer(): Future[bool] {.async.} =
  #       const
  #         proto = "/noise/xx/25519/chachapoly/sha256/0.1.0"

  #       let
  #         local = Multiaddress.init("/ip4/0.0.0.0/tcp/0")
  #         remote = Multiaddress.init("/ip4/127.0.0.1/tcp/23456")
  #         info = PeerInfo.init(PrivateKey.random(ECDSA), [local])
  #         noise = newNoise(info.privateKey)
  #         ms = newMultistream()
  #         transport = TcpTransport.newTransport()
  #         conn = await transport.dial(remote)

  #       check ms.select(conn, @[proto]).await == proto

  #       let
  #         sconn = await noise.secure(conn, true)

  #       # use sconn

  #       result = true

  #     check:
  #       waitFor(testListenerDialer()) == true

  # test "interop with go noise":
  #   when true: # disable cos in CI we got no interop server/client
  #     proc testListenerDialer(): Future[bool] {.async.} =
  #       let
  #         local = Multiaddress.init("/ip4/0.0.0.0/tcp/23456")
  #         info = PeerInfo.init(PrivateKey.random(ECDSA), [local])
  #         noise = newNoise(info.privateKey)
  #         ms = newMultistream()
  #         transport = TcpTransport.newTransport()

  #       proc connHandler(conn: Connection) {.async, gcsafe.} =
  #         try:
  #           let seconn = await noise.secure(conn, false)
  #           trace "ms.handle exited"
  #         finally:
  #           await conn.close()

  #       let
  #         clientConn = await transport.listen(local, connHandler)
  #       await clientConn

  #       result = true

  #     check:
  #       waitFor(testListenerDialer()) == true
