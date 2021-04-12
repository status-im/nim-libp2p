import chronos, stew/byteutils
import ../libp2p/stream/bufferstream,
       ../libp2p/stream/lpstream,
       ../libp2p/errors

import ./helpers
import ./asyncunit

{.used.}

suite "BufferStream":
  teardown:
    # echo getTracker(BufferStreamTrackerName).dump()
    check getTracker(BufferStreamTrackerName).isLeaked() == false

  asyncTest "push data to buffer":
    let buff = newBufferStream()
    check buff.len == 0
    var data = "12345"
    await buff.pushData(data.toBytes())
    check buff.len == 5
    await buff.close()

  asyncTest "push and wait":
    let buff = newBufferStream()
    check buff.len == 0

    let fut0 = buff.pushData("1234".toBytes())
    let fut1 = buff.pushData("5".toBytes())
    check buff.len == 4 # the second write should not be visible yet

    var data: array[1, byte]
    check: 1 == await buff.readOnce(addr data[0], data.len)

    check ['1'] == string.fromBytes(data)
    await fut0
    await fut1
    check buff.len == 4
    await buff.close()

  asyncTest "read with size":
    let buff = newBufferStream()
    check buff.len == 0

    await buff.pushData("12345".toBytes())
    var data: array[3, byte]
    await buff.readExactly(addr data[0], data.len)
    check ['1', '2', '3'] == string.fromBytes(data)
    await buff.close()

  asyncTest "readExactly":
    let buff = newBufferStream()
    check buff.len == 0

    await buff.pushData("12345".toBytes())
    check buff.len == 5
    var data: array[2, byte]
    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == ['1', '2']
    await buff.close()

  asyncTest "readExactly raises":
    let buff = newBufferStream()
    check buff.len == 0

    await buff.pushData("123".toBytes())
    var data: array[5, byte]
    var readFut = buff.readExactly(addr data[0], data.len)
    await buff.close()

    expect LPStreamIncompleteError:
      await readFut

  asyncTest "readOnce":
    let buff = newBufferStream()
    check buff.len == 0

    var data: array[3, byte]
    let readFut = buff.readOnce(addr data[0], data.len)
    await buff.pushData("123".toBytes())
    check buff.len == 3

    check (await readFut) == 3
    check string.fromBytes(data) == ['1', '2', '3']
    await buff.close()

  asyncTest "reads should happen in order":
    let buff = newBufferStream()
    check buff.len == 0

    proc writer1() {.async.} =
      await buff.pushData("Msg 1".toBytes())
      await buff.pushData("Msg 2".toBytes())
      await buff.pushData("Msg 3".toBytes())

    let writerFut1 = writer1()
    var data: array[5, byte]
    await buff.readExactly(addr data[0], data.len)

    check string.fromBytes(data) == "Msg 1"

    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == "Msg 2"

    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == "Msg 3"

    await writerFut1

    proc writer2() {.async.} =
      await buff.pushData("Msg 4".toBytes())
      await buff.pushData("Msg 5".toBytes())
      await buff.pushData("Msg 6".toBytes())

    let writerFut2 = writer2()

    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == "Msg 4"

    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == "Msg 5"

    await buff.readExactly(addr data[0], data.len)
    check string.fromBytes(data) == "Msg 6"

    await buff.close()
    await writerFut2

  asyncTest "small reads":
    let buff = newBufferStream()
    check buff.len == 0

    var str: string
    proc writer() {.async.} =
      for i in 0..<10:
        await buff.pushData("123".toBytes())
        str &= "123"
      await buff.close() # all data should still be read after close

    var str2: string

    proc reader() {.async.} =
      var data: array[2, byte]
      expect LPStreamEOFError:
        while true:
          let x = await buff.readOnce(addr data[0], data.len)
          str2 &= string.fromBytes(data[0..<x])


    await allFuturesThrowing(
      allFinished(reader(), writer()))
    check str == str2
    await buff.close()

  asyncTest "read all data after eof":
    let buff = newBufferStream()
    check buff.len == 0

    await buff.pushData("12345".toBytes())
    var data: array[2, byte]
    check: (await buff.readOnce(addr data[0], data.len)) == 2

    await buff.pushEof()

    check:
      not buff.atEof()
      (await buff.readOnce(addr data[0], data.len)) == 2
      not buff.atEof()
      (await buff.readOnce(addr data[0], data.len)) == 1
      buff.atEof()
      # exactly one 0-byte read
      (await buff.readOnce(addr data[0], data.len)) == 0

    expect LPStreamEOFError:
      discard (await buff.readOnce(addr data[0], data.len))

    await buff.close() # all data should still be read after close

  asyncTest "read more data after eof":
    let buff = newBufferStream()
    check buff.len == 0

    await buff.pushData("12345".toBytes())
    var data: array[5, byte]
    check: (await buff.readOnce(addr data[0], 1)) == 1 # 4 bytes in readBuf

    await buff.pushEof()

    check:
      not buff.atEof()
      (await buff.readOnce(addr data[0], 1)) == 1 # 3 bytes in readBuf, eof marker processed
      not buff.atEof()
      (await buff.readOnce(addr data[0], data.len)) == 3 # 0 bytes in readBuf
      buff.atEof()
      # exactly one 0-byte read
      (await buff.readOnce(addr data[0], data.len)) == 0

    expect LPStreamEOFError:
      discard (await buff.readOnce(addr data[0], data.len))

    await buff.close() # all data should still be read after close

  asyncTest "shouldn't get stuck on close":
    var stream = newBufferStream()
    var
      fut = stream.pushData(toBytes("hello"))
      fut2 = stream.pushData(toBytes("again"))
    await stream.close()

    # Both writes should be completed on close (technically, the should maybe
    # be cancelled, at least the second one...
    check await fut.withTimeout(100.milliseconds)
    check await fut2.withTimeout(100.milliseconds)

    await stream.close()

  asyncTest "no push after close":
    var stream = newBufferStream()
    await stream.pushData("123".toBytes())
    var data: array[3, byte]
    await stream.readExactly(addr data[0], data.len)
    await stream.close()

    expect LPStreamEOFError:
      await stream.pushData("123".toBytes())

  asyncTest "no concurrent pushes":
    var stream = newBufferStream()
    await stream.pushData("123".toBytes())
    let push = stream.pushData("123".toBytes())

    expect AssertionError:
      await stream.pushData("123".toBytes())

    await stream.closeWithEOF()
    await push
