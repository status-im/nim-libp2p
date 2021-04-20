import strformat, strformat, random, oids, sequtils
import chronos, nimcrypto/utils, chronicles, stew/byteutils
import ../libp2p/[errors,
                  stream/connection,
                  stream/bufferstream,
                  transports/tcptransport,
                  transports/transport,
                  multiaddress,
                  muxers/mplex/mplex,
                  muxers/mplex/coder,
                  muxers/mplex/lpchannel,
                  upgrademngrs/upgrade,
                  vbuffer,
                  varint]

import ./helpers

{.used.}

suite "Mplex":
  teardown:
    checkTrackers()

  suite "channel encoding":
    asyncTest "encode header with channel id 0":
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("000873747265616d2031")

      let conn = TestBufferStream.new(encHandler)
      await conn.writeMsg(0, MessageType.New, ("stream 1").toBytes)
      await conn.close()

    asyncTest "encode header with channel id other than 0":
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("88010873747265616d2031")

      let conn = TestBufferStream.new(encHandler)
      await conn.writeMsg(17, MessageType.New, ("stream 1").toBytes)
      await conn.close()

    asyncTest "encode header and body with channel id 0":
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("020873747265616d2031")

      let conn = TestBufferStream.new(encHandler)
      await conn.writeMsg(0, MessageType.MsgOut, ("stream 1").toBytes)
      await conn.close()

    asyncTest "encode header and body with channel id other than 0":
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("8a010873747265616d2031")

      let conn = TestBufferStream.new(encHandler)
      await conn.writeMsg(17, MessageType.MsgOut, ("stream 1").toBytes)
      await conn.close()

    asyncTest "decode header with channel id 0":
      let stream = BufferStream.new()
      let conn = stream
      await stream.pushData(fromHex("000873747265616d2031"))
      let msg = await conn.readMsg()

      check msg.id == 0
      check msg.msgType == MessageType.New
      await conn.close()

    asyncTest "decode header and body with channel id 0":
      let stream = BufferStream.new()
      let conn = stream
      await stream.pushData(fromHex("021668656C6C6F2066726F6D206368616E6E656C20302121"))
      let msg = await conn.readMsg()

      check msg.id == 0
      check msg.msgType == MessageType.MsgOut
      check string.fromBytes(msg.data) == "hello from channel 0!!"
      await conn.close()

    asyncTest "decode header and body with channel id other than 0":
      let stream = BufferStream.new()
      let conn = stream
      await stream.pushData(fromHex("8a011668656C6C6F2066726F6D206368616E6E656C20302121"))
      let msg = await conn.readMsg()

      check msg.id == 17
      check msg.msgType == MessageType.MsgOut
      check string.fromBytes(msg.data) == "hello from channel 0!!"
      await conn.close()

  suite "channel half-closed":
    asyncTest "(local close) - should close for write":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      await chann.close()
      expect LPStreamClosedError:
        await chann.write("Hello")

      await chann.reset()
      await conn.close()

    asyncTest "(local close) - should allow reads until remote closes":
      let
        conn = TestBufferStream.new(
          proc (data: seq[byte]) {.gcsafe, async.} =
            discard,
        )
        chann = LPChannel.init(1, conn, true)

      await chann.pushData(("Hello!").toBytes)

      var data = newSeq[byte](6)
      await chann.close() # closing channel
      # should be able to read on local clsoe
      await chann.readExactly(addr data[0], 3)
      # closing remote end
      let closeFut = chann.pushEof()
      # should still allow reading until buffer EOF
      await chann.readExactly(addr data[3], 3)

      expect LPStreamEOFError:
        # this should fail now
        await chann.readExactly(addr data[0], 3)

      await chann.close()
      await conn.close()
      await closeFut

    asyncTest "(remote close) - channel should close for reading by remote":
      let
        conn = TestBufferStream.new(
          proc (data: seq[byte]) {.gcsafe, async.} =
            discard,
        )
        chann = LPChannel.init(1, conn, true)

      await chann.pushData(("Hello!").toBytes)

      var data = newSeq[byte](6)
      await chann.readExactly(addr data[0], 3)
      let closeFut = chann.pushEof() # closing channel
      let readFut = chann.readExactly(addr data[3], 3)
      await allFutures(closeFut, readFut)

      expect LPStreamEOFError:
        await chann.readExactly(addr data[0], 6) # this should fail now

      await chann.close()
      await conn.close()

    asyncTest "(remote close) - channel should allow writing on remote close":
      let
        testData = "Hello!".toBytes
        conn = TestBufferStream.new(
          proc (data: seq[byte]) {.gcsafe, async.} =
            discard
        )
        chann = LPChannel.init(1, conn, true)

      await chann.pushEof() # closing channel
      try:
        await chann.writeLp(testData)
      finally:
        await chann.reset() # there's nobody reading the EOF!
        await conn.close()

    asyncTest "should not allow pushing data to channel when remote end closed":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)
      await chann.pushEof()
      var buf: array[1, byte]
      check: (await chann.readOnce(addr buf[0], 1)) == 0 # EOF marker read

      expect LPStreamEOFError:
        await chann.pushData(@[byte(1)])

      await chann.close()
      await conn.close()

  suite "channel reset":

    asyncTest "channel should fail reading":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      await chann.reset()
      var data = newSeq[byte](1)
      expect LPStreamEOFError:
        await chann.readExactly(addr data[0], 1)

      await conn.close()

    asyncTest "reset should complete read":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let fut = chann.readExactly(addr data[0], 1)

      await chann.reset()
      expect LPStreamEOFError:
        await fut

      await conn.close()

    asyncTest "reset should complete pushData":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      proc pushes() {.async.} = # pushes don't hang on reset
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])

      let push = pushes()
      await chann.reset()
      check await allFutures(push).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should complete both read and push":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let futs = [
        chann.readExactly(addr data[0], 1),
        chann.pushData(@[0'u8]),
      ]
      await chann.reset()
      check await allFutures(futs).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should complete both read and pushes":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let read = chann.readExactly(addr data[0], 1)
      proc pushes() {.async.} =
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])

      await chann.reset()
      check await allFutures(read, pushes()).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should complete both read and push with cancel":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let rfut = chann.readExactly(addr data[0], 1)
      rfut.cancel()
      let xfut = chann.reset()

      check await allFutures(rfut, xfut).withTimeout(100.millis)
      await conn.close()

    asyncTest "should complete both read and push after reset":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let rfut = chann.readExactly(addr data[0], 1)
      let rfut2 = sleepAsync(1.millis) or rfut

      await sleepAsync(5.millis)

      let wfut = chann.pushData(@[0'u8])
      let wfut2 = chann.pushData(@[0'u8])
      await chann.reset()
      check await allFutures(rfut, rfut2, wfut, wfut2).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should complete ongoing push without reader":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      await chann.pushData(@[0'u8])
      let push1 = chann.pushData(@[0'u8])
      await chann.reset()
      check await allFutures(push1).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should complete ongoing read without a push":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      let rfut = chann.readExactly(addr data[0], 1)
      await chann.reset()
      check await allFutures(rfut).withTimeout(100.millis)
      await conn.close()

    asyncTest "reset should allow all reads and pushes to complete":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)

      var data = newSeq[byte](1)
      proc writer() {.async.} =
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])
        await chann.pushData(@[0'u8])

      proc reader() {.async.} =
        await chann.readExactly(addr data[0], 1)
        await chann.readExactly(addr data[0], 1)
        await chann.readExactly(addr data[0], 1)

      let rw = @[writer(), reader()]

      await chann.close()
      check await chann.reset() # this would hang
      .withTimeout(100.millis)

      check await allFuturesThrowing(
        allFinished(rw))
        .withTimeout(100.millis)

      await conn.close()

    asyncTest "channel should fail writing":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(1, conn, true)
      await chann.reset()

      expect LPStreamClosedError:
        await chann.write(("Hello!").toBytes)

      await conn.close()

    asyncTest "channel should reset on timeout":
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let
        conn = TestBufferStream.new(writeHandler)
        chann = LPChannel.init(
          1, conn, true, timeout = 100.millis)

      check await chann.join().withTimeout(1.minutes)
      await conn.close()

  suite "mplex e2e":
    asyncTest "read/write receiver":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          let msg = await stream.readLp(1024)
          check string.fromBytes(msg) == "HELLO"
          await stream.close()

        await mplexListen.handle()
        await mplexListen.close()

      let acceptFut = acceptHandler()
      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      let stream = await mplexDial.newStream()
      await stream.writeLp("HELLO")
      check LPChannel(stream).isOpen # not lazy
      await stream.close()

      await conn.close()
      await acceptFut.wait(1.seconds)
      await mplexDialFut.wait(1.seconds)
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await listenFut

    asyncTest "read/write receiver lazy":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          let msg = await stream.readLp(1024)
          check string.fromBytes(msg) == "HELLO"
          await stream.close()

        await mplexListen.handle()
        await mplexListen.close()

      let acceptFut = acceptHandler()
      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let stream = await mplexDial.newStream(lazy = true)
      let mplexDialFut = mplexDial.handle()
      check not LPChannel(stream).isOpen # assert lazy
      await stream.writeLp("HELLO")
      check LPChannel(stream).isOpen # assert lazy
      await stream.close()

      await conn.close()
      await acceptFut.wait(1.seconds)
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await listenFut

    asyncTest "write fragmented":
      let
        ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
        listenJob = newFuture[void]()

      var bigseq = newSeqOfCap[uint8](MaxMsgSize * 2)
      for _ in 0..<MaxMsgSize:
        bigseq.add(uint8(rand(uint('A')..uint('z'))))

      let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        try:
          let conn = await transport1.acceptStream()
          let mplexListen = Mplex.init(conn)
          mplexListen.streamHandler = proc(stream: Connection)
            {.async, gcsafe.} =
            let msg = await stream.readLp(MaxMsgSize)
            check msg == bigseq
            trace "Bigseq check passed!"
            await stream.close()
            listenJob.complete()

          await mplexListen.handle()
          await sleepAsync(1.seconds) # give chronos some slack to process things
          await mplexListen.close()
        except CancelledError as exc:
          raise exc
        except CatchableError as exc:
          check false

      let acceptFut = acceptHandler()
      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      let stream  = await mplexDial.newStream()

      await stream.writeLp(bigseq)
      await listenJob.wait(10.seconds)

      await stream.close()
      await conn.close()
      await acceptFut
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())

      await listenFut

    asyncTest "read/write initiator":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          await stream.writeLp("Hello from stream!")
          await stream.close()

        await mplexListen.handle()
        await mplexListen.close()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let acceptFut = acceptHandler()
      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      let stream  = await mplexDial.newStream("DIALER")
      let msg = string.fromBytes(await stream.readLp(1024))
      await stream.close()
      check msg == "Hello from stream!"

      await conn.close()
      await acceptFut.wait(1.seconds)
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await listenFut

    asyncTest "multiple streams":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1 = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      let done = newFuture[void]()
      proc acceptHandler() {.async, gcsafe.} =
        var count = 1
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          let msg = await stream.readLp(1024)
          check string.fromBytes(msg) == &"stream {count}!"
          count.inc
          if count == 11:
            done.complete()
          await stream.close()

        await mplexListen.handle()
        await mplexListen.close()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let acceptFut = acceptHandler()
      let mplexDial = Mplex.init(conn)
      # TODO: Reenable once half-closed is working properly
      let mplexDialFut = mplexDial.handle()
      for i in 1..10:
        let stream  = await mplexDial.newStream()
        await stream.writeLp(&"stream {i}!")
        await stream.close()

      await done.wait(10.seconds)
      await conn.close()
      await acceptFut.wait(1.seconds)
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await listenFut

    asyncTest "multiple read/write streams":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let listenFut = transport1.start(ma)

      let done = newFuture[void]()
      proc acceptHandler() {.async, gcsafe.} =
        var count = 1
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          let msg = await stream.readLp(1024)
          check string.fromBytes(msg) == &"stream {count} from dialer!"
          await stream.writeLp(&"stream {count} from listener!")
          count.inc
          if count == 11:
            done.complete()
          await stream.close()

        await mplexListen.handle()
        await mplexListen.close()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let acceptFut = acceptHandler()
      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      for i in 1..10:
        let stream  = await mplexDial.newStream("dialer stream")
        await stream.writeLp(&"stream {i} from dialer!")
        let msg = await stream.readLp(1024)
        check string.fromBytes(msg) == &"stream {i} from listener!"
        await stream.close()

      await done.wait(5.seconds)
      await conn.close()
      await acceptFut.wait(1.seconds)
      await mplexDialFut
      await mplexDial.close()
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await listenFut

    asyncTest "channel closes listener with EOF":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

      let transport1 = TcpTransport.init(upgrade = Upgrade())
      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)

        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          listenStreams.add(stream)
          try:
            discard await stream.readLp(1024)
          except LPStreamEOFError:
            await stream.close()
            return

          check false

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()
      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      for i, s in dialStreams:
        await s.closeWithEOF()
        check listenStreams[i].closed
        check s.closed

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    asyncTest "channel closes dialer with EOF":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var count = 0
      var done = newFuture[void]()
      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
          listenStreams.add(stream)
          count.inc()
          if count == 10:
            done.complete()

          await stream.join()

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      proc dialReadLoop() {.async.} =
        for s in dialStreams:
          try:
            discard await s.readLp(1024)
            check false
          except LPStreamEOFError:
            await s.close()
            continue

          check false

      await done
      let readLoop = dialReadLoop()
      for s in listenStreams:
        await s.closeWithEOF()
        check s.closed

      await readLoop
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    asyncTest "dialing mplex closes both ends":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
            listenStreams.add(stream)
            await stream.join()

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      await mplexDial.close()
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    asyncTest "listening mplex closes both ends":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var mplexListen: Mplex
      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
            listenStreams.add(stream)
            await stream.join()

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      await mplexListen.close()
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    asyncTest "canceling mplex handler closes both ends":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var mplexHandle: Future[void]
      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
            listenStreams.add(stream)
            await stream.join()

        mplexHandle = mplexListen.handle()
        await mplexHandle
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      mplexHandle.cancel()
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())

    asyncTest "closing dialing connection should close both ends":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.acceptStream()
        let mplexListen = Mplex.init(conn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
            listenStreams.add(stream)
            await stream.join()

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      await conn.close()
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.closeWithEOF()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    asyncTest "canceling listening connection should close both ends":
      let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
      let transport1 = TcpTransport.init(upgrade = Upgrade())

      var listenConn: Connection
      var listenStreams: seq[Connection]
      proc acceptHandler() {.async, gcsafe.} =
        listenConn = await transport1.acceptStream()
        let mplexListen = Mplex.init(listenConn)
        mplexListen.streamHandler = proc(stream: Connection)
          {.async, gcsafe.} =
            listenStreams.add(stream)
            await stream.join()

        await mplexListen.handle()
        await mplexListen.close()

      await transport1.start(ma)
      let acceptFut = acceptHandler()

      let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
      let conn = await transport2.dialStream(transport1.ma)

      let mplexDial = Mplex.init(conn)
      let mplexDialFut = mplexDial.handle()
      var dialStreams: seq[Connection]
      for i in 0..9:
        dialStreams.add((await mplexDial.newStream()))

      await sleepAsync(100.millis)
      await listenConn.closeWithEOF()
      await allFuturesThrowing(
          (dialStreams & listenStreams)
          .mapIt( it.join() ))

      checkTracker(LPChannelTrackerName)

      await conn.close()
      await mplexDialFut
      await allFuturesThrowing(
        transport1.stop(),
        transport2.stop())
      await acceptFut

    suite "jitter":
      asyncTest "channel should be able to handle erratic read/writes":
        let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

        let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        let listenFut = transport1.start(ma)

        var complete = newFuture[void]()
        const MsgSize = 1024
        proc acceptHandler() {.async, gcsafe.} =
          let conn = await transport1.acceptStream()
          let mplexListen = Mplex.init(conn)
          mplexListen.streamHandler = proc(stream: Connection)
            {.async, gcsafe.} =
            try:
              let msg = await stream.readLp(MsgSize)
              check msg.len == MsgSize
            except CatchableError as e:
              echo e.msg
            await stream.close()
            complete.complete()

          await mplexListen.handle()
          await mplexListen.close()

        let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        let conn = await transport2.dialStream(transport1.ma)

        let acceptFut = acceptHandler()
        let mplexDial = Mplex.init(conn)
        let mplexDialFut = mplexDial.handle()
        let stream = await mplexDial.newStream()
        var bigseq = newSeqOfCap[uint8](MaxMsgSize + 1)
        for _ in 0..<MsgSize: # write one less than max size
          bigseq.add(uint8(rand(uint('A')..uint('z'))))

        ## create length prefixed libp2p frame
        var buf = initVBuffer()
        buf.writeSeq(bigseq)
        buf.finish()

        ## create mplex header
        var mplexBuf = initVBuffer()
        mplexBuf.writePBVarint((1.uint shl 3) or ord(MessageType.MsgOut).uint)
        mplexBuf.writePBVarint(buf.buffer.len.uint) # size should be always sent

        await conn.write(mplexBuf.buffer)
        proc writer() {.async.} =
          var sent = 0
          randomize()
          let total = buf.buffer.len
          const min = 20
          const max = 50
          while sent < total:
            var size = rand(min..max)
            size = if size > buf.buffer.len: buf.buffer.len else: size
            var send = buf.buffer[0..<size]
            await conn.write(send)
            sent += size
            buf.buffer = buf.buffer[size..^1]

        await writer()
        await complete.wait(1.seconds)
        await stream.close()
        await conn.close()
        await acceptFut
        await mplexDialFut

        await allFuturesThrowing(
          transport1.stop(),
          transport2.stop())
        await listenFut

      asyncTest "channel should handle 1 byte read/write":
        let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

        let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        let listenFut = transport1.start(ma)

        var complete = newFuture[void]()
        const MsgSize = 512
        proc acceptHandler() {.async, gcsafe.} =
          let conn = await transport1.acceptStream()
          let mplexListen = Mplex.init(conn)
          mplexListen.streamHandler = proc(stream: Connection)
            {.async, gcsafe.} =
            let msg = await stream.readLp(MsgSize)
            check msg.len == MsgSize
            await stream.close()
            complete.complete()

          await mplexListen.handle()
          await mplexListen.close()

        let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
        let conn = await transport2.dialStream(transport1.ma)

        let acceptFut = acceptHandler()
        let mplexDial = Mplex.init(conn)
        let stream = await mplexDial.newStream()
        let mplexDialFut = mplexDial.handle()
        var bigseq = newSeqOfCap[uint8](MsgSize + 1)
        for _ in 0..<MsgSize: # write one less than max size
          bigseq.add(uint8(rand(uint('A')..uint('z'))))

        ## create length prefixed libp2p frame
        var buf = initVBuffer()
        buf.writeSeq(bigseq)
        buf.finish()

        ## create mplex header
        var mplexBuf = initVBuffer()
        mplexBuf.writePBVarint((1.uint shl 3) or ord(MessageType.MsgOut).uint)
        mplexBuf.writePBVarint(buf.buffer.len.uint) # size should be always sent

        await conn.write(mplexBuf.buffer)
        proc writer() {.async.} =
          for i in buf.buffer:
            await conn.write(@[i])

        await writer()

        await complete.wait(5.seconds)
        await stream.close()
        await conn.close()
        await acceptFut
        await mplexDialFut
        await allFuturesThrowing(
          transport1.stop(),
          transport2.stop())
        await listenFut
