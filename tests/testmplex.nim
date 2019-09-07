import unittest, sequtils, sugar, strformat
import chronos, nimcrypto/utils
import ../libp2p/connection, 
       ../libp2p/stream/lpstream, 
       ../libp2p/stream/bufferstream, 
       ../libp2p/transports/tcptransport, 
       ../libp2p/transports/transport, 
       ../libp2p/protocols/identify, 
       ../libp2p/multiaddress,
       ../libp2p/muxers/mplex/mplex, 
       ../libp2p/muxers/mplex/coder, 
       ../libp2p/muxers/mplex/types,
       ../libp2p/muxers/mplex/channel

suite "Mplex":
  test "encode header with channel id 0":
    proc testEncodeHeader(): Future[bool] {.async.} =
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("000873747265616d2031")
      
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await conn.writeMsg(0, MessageType.New, cast[seq[byte]](toSeq("stream 1".items)))
      result = true

    check:
      waitFor(testEncodeHeader()) == true

  test "encode header with channel id other than 0":
    proc testEncodeHeader(): Future[bool] {.async.} =
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("88010873747265616d2031")
      
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await conn.writeMsg(17, MessageType.New, cast[seq[byte]](toSeq("stream 1".items)))
      result = true

    check:
      waitFor(testEncodeHeader()) == true

  test "encode header and body with channel id 0":
    proc testEncodeHeaderBody(): Future[bool] {.async.} =
      var step = 0
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("020873747265616d2031")
      
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await conn.writeMsg(0, MessageType.MsgOut, cast[seq[byte]](toSeq("stream 1".items)))
      result = true

    check:
      waitFor(testEncodeHeaderBody()) == true

  test "encode header and body with channel id other than 0":
    proc testEncodeHeaderBody(): Future[bool] {.async.} =
      var step = 0
      proc encHandler(msg: seq[byte]) {.async.} =
        check msg == fromHex("8a010873747265616d2031")

      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await conn.writeMsg(17, MessageType.MsgOut, cast[seq[byte]](toSeq("stream 1".items)))
      await conn.close()
      result = true

    check:
      waitFor(testEncodeHeaderBody()) == true

  test "decode header with channel id 0":
    proc testDecodeHeader(): Future[bool] {.async.} =
      proc encHandler(msg: seq[byte]) {.async.} = discard 
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await stream.pushTo(fromHex("000873747265616d2031"))
      let (id, msgType, data) = await conn.readMsg()

      check id == 0
      check msgType == MessageType.New
      result = true

    check:
      waitFor(testDecodeHeader()) == true

  test "decode header and body with channel id 0":
    proc testDecodeHeader(): Future[bool] {.async.} =
      proc encHandler(msg: seq[byte]) {.async.} = discard 
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await stream.pushTo(fromHex("021668656C6C6F2066726F6D206368616E6E656C20302121"))
      let (id, msgType, data) = await conn.readMsg()

      check id == 0
      check msgType == MessageType.MsgOut
      check cast[string](data) == "hello from channel 0!!"
      result = true

    check:
      waitFor(testDecodeHeader()) == true

  test "decode header and body with channel id other than 0":
    proc testDecodeHeader(): Future[bool] {.async.} =
      proc encHandler(msg: seq[byte]) {.async.} = discard 
      let stream = newBufferStream(encHandler)
      let conn = newConnection(stream)
      await stream.pushTo(fromHex("8a011668656C6C6F2066726F6D206368616E6E656C20302121"))
      let (id, msgType, data) = await conn.readMsg()

      check id == 17
      check msgType == MessageType.MsgOut
      check cast[string](data) == "hello from channel 0!!"
      result = true

    check:
      waitFor(testDecodeHeader()) == true

  test "e2e - read/write initiator":
    proc testNewStream(): Future[bool] {.async.} =
      let ma: MultiAddress = Multiaddress.init("/ip4/127.0.0.1/tcp/53380")

      proc connHandler(conn: Connection) {.async, gcsafe.} =
        proc handleMplexListen(stream: Connection) {.async, gcsafe.} = 
          await stream.writeLp("Hello from stream!")
          await stream.close()

        let mplexListen = newMplex(conn)
        mplexListen.streamHandler = handleMplexListen
        await mplexListen.handle()

      let transport1: TcpTransport = newTransport(TcpTransport)
      await transport1.listen(ma, connHandler)

      let transport2: TcpTransport = newTransport(TcpTransport)
      let conn = await transport2.dial(ma)

      let mplexDial = newMplex(conn)
      let dialFut = mplexDial.handle()
      let stream  = await mplexDial.newStream("DIALER")
      let msg = cast[string](await stream.readLp())
      check msg == "Hello from stream!"
      await conn.close()
      # await dialFut
      result = true

    check:
      waitFor(testNewStream()) == true

  test "e2e - read/write receiver":
    proc testNewStream(): Future[bool] {.async.} =
      let ma: MultiAddress = Multiaddress.init("/ip4/127.0.0.1/tcp/53381")

      proc connHandler(conn: Connection) {.async, gcsafe.} =
        proc handleMplexListen(stream: Connection) {.async, gcsafe.} = 
          let msg = await stream.readLp()
          check cast[string](msg) == "Hello from stream!"
          await stream.close()

        let mplexListen = newMplex(conn)
        mplexListen.streamHandler = handleMplexListen
        await mplexListen.handle()

      let transport1: TcpTransport = newTransport(TcpTransport)
      await transport1.listen(ma, connHandler)

      let transport2: TcpTransport = newTransport(TcpTransport)
      let conn = await transport2.dial(ma)

      let mplexDial = newMplex(conn)
      let stream  = await mplexDial.newStream()
      await stream.writeLp("Hello from stream!")
      await conn.close()
      result = true

    check:
      waitFor(testNewStream()) == true

  test "e2e - multiple streams":
    proc testNewStream(): Future[bool] {.async.} =
      let ma: MultiAddress = Multiaddress.init("/ip4/127.0.0.1/tcp/53382")

      var count = 1
      proc connHandler(conn: Connection) {.async, gcsafe.} =
        proc handleMplexListen(stream: Connection) {.async, gcsafe.} = 
          let msg = await stream.readLp()
          check cast[string](msg) == &"stream {count}!"
          count.inc
          await stream.close()

        let mplexListen = newMplex(conn)
        mplexListen.streamHandler = handleMplexListen
        asyncCheck mplexListen.handle()

      let transport1: TcpTransport = newTransport(TcpTransport)
      await transport1.listen(ma, connHandler)

      let transport2: TcpTransport = newTransport(TcpTransport)
      let conn = await transport2.dial(ma)

      let mplexDial = newMplex(conn)
      for i in 1..<10:
        let stream  = await mplexDial.newStream()
        await stream.writeLp(&"stream {i}!")
        await stream.close()

      await conn.close()
      result = true

    check:
      waitFor(testNewStream()) == true

  test "half closed - channel should close for write":
    proc testClosedForWrite(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)
      await chann.close()
      await chann.write("Hello")

    expect LPStreamClosedError:
      waitFor(testClosedForWrite())

  test "half closed - channel should close for read":
    proc testClosedForRead(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)
      await chann.closedByRemote()
      asyncDiscard chann.read()

    expect LPStreamClosedError:
      waitFor(testClosedForRead())

  test "half closed - channel should close for read after eof":
    proc testClosedForRead(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)

      await chann.pushTo(cast[seq[byte]](toSeq("Hello!".items)))
      await chann.close()
      let msg = await chann.read()
      asyncDiscard chann.read()

    expect LPStreamClosedError:
      waitFor(testClosedForRead())

  test "reset - channel should fail reading":
    proc testResetRead(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)
      await chann.reset()
      asyncDiscard chann.read()

    expect LPStreamClosedError:
      waitFor(testResetRead())

  test "reset - channel should fail writing":
    proc testResetWrite(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)
      await chann.reset()
      asyncDiscard chann.read()

    expect LPStreamClosedError:
      waitFor(testResetWrite())

  test "should not allow pushing data to channel when remote end closed":
    proc testResetWrite(): Future[void] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let chann = newChannel(1, newConnection(newBufferStream(writeHandler)), true)
      await chann.closedByRemote()
      await chann.pushTo(@[byte(1)])

    expect LPStreamClosedError:
      waitFor(testResetWrite())
