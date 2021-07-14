{.used.}

import sequtils
import chronos, stew/byteutils
import ../libp2p/[stream/connection,
                  transports/transport,
                  transports/tcptransport,
                  upgrademngrs/upgrade,
                  multiaddress,
                  errors,
                  wire]

import ./helpers, ./commontransport

suite "TCP transport":
  teardown:
    checkTrackers()

  asyncTest "test listener: handle write":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
    let transport: TcpTransport = TcpTransport.new(upgrade = Upgrade())
    asyncSpawn transport.start(ma)

    proc acceptHandler() {.async, gcsafe.} =
      let conn = await transport.accept()
      await conn.write("Hello!")
      await conn.close()

    let handlerWait = acceptHandler()

    let streamTransport = await connect(transport.ma)

    let msg = await streamTransport.read(6)

    await handlerWait.wait(1.seconds) # when no issues will not wait that long!
    await streamTransport.closeWait()
    await transport.stop()
    check string.fromBytes(msg) == "Hello!"

  asyncTest "test listener: handle read":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    let transport: TcpTransport = TcpTransport.new(upgrade = Upgrade())
    asyncSpawn transport.start(ma)

    proc acceptHandler() {.async, gcsafe.} =
      var msg = newSeq[byte](6)
      let conn = await transport.accept()
      await conn.readExactly(addr msg[0], 6)
      check string.fromBytes(msg) == "Hello!"
      await conn.close()

    let handlerWait = acceptHandler()
    let streamTransport: StreamTransport = await connect(transport.ma)
    let sent = await streamTransport.write("Hello!")

    await handlerWait.wait(1.seconds) # when no issues will not wait that long!
    await streamTransport.closeWait()
    await transport.stop()

    check sent == 6

  asyncTest "test dialer: handle write":
    let address = initTAddress("0.0.0.0:0")
    let handlerWait = newFuture[void]()
    proc serveClient(server: StreamServer,
                      transp: StreamTransport) {.async, gcsafe.} =
      var wstream = newAsyncStreamWriter(transp)
      await wstream.write("Hello!")
      await wstream.finish()
      await wstream.closeWait()
      await transp.closeWait()
      server.stop()
      server.close()
      handlerWait.complete()

    var server = createStreamServer(address, serveClient, {ReuseAddr})
    server.start()

    let ma: MultiAddress = MultiAddress.init(server.sock.getLocalAddress()).tryGet()
    let transport: TcpTransport = TcpTransport.new(upgrade = Upgrade())
    let conn = await transport.dial(ma)
    var msg = newSeq[byte](6)
    await conn.readExactly(addr msg[0], 6)
    check string.fromBytes(msg) == "Hello!"

    await handlerWait.wait(1.seconds) # when no issues will not wait that long!

    await conn.close()
    await transport.stop()

    server.stop()
    server.close()
    await server.join()

  asyncTest "test dialer: handle write":
    let address = initTAddress("0.0.0.0:0")
    let handlerWait = newFuture[void]()
    proc serveClient(server: StreamServer,
                      transp: StreamTransport) {.async, gcsafe.} =
      var rstream = newAsyncStreamReader(transp)
      let msg = await rstream.read(6)
      check string.fromBytes(msg) == "Hello!"

      await rstream.closeWait()
      await transp.closeWait()
      server.stop()
      server.close()
      handlerWait.complete()

    var server = createStreamServer(address, serveClient, {ReuseAddr})
    server.start()

    let ma: MultiAddress = MultiAddress.init(server.sock.getLocalAddress()).tryGet()
    let transport: TcpTransport = TcpTransport.new(upgrade = Upgrade())
    let conn = await transport.dial(ma)
    await conn.write("Hello!")

    await handlerWait.wait(1.seconds) # when no issues will not wait that long!

    await conn.close()
    await transport.stop()

    server.stop()
    server.close()
    await server.join()

  commonTransportTest(
    "TcpTransport",
    proc (): Transport = TcpTransport.new(upgrade = Upgrade()),
    "/ip4/0.0.0.0/tcp/0")
