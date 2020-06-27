import unittest, strformat
import chronos, stew/byteutils
import ../libp2p/stream/bufferstream,
       ../libp2p/stream/lpstream,
       ../libp2p/errors

when defined(nimHasUsed): {.used.}

suite "BufferStream":
  teardown:
    # echo getTracker("libp2p.bufferstream").dump()
    check getTracker("libp2p.bufferstream").isLeaked() == false

  test "push data to buffer":
    proc testPushTo(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 16)
      check buff.len == 0
      var data = "12345"
      await buff.pushTo(data.toBytes())
      check buff.len == 5
      result = true

      await buff.close()

    check:
      waitFor(testPushTo()) == true

  test "push and wait":
    proc testPushTo(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 4)
      check buff.len == 0

      let fut = buff.pushTo("12345".toBytes())
      check buff.len == 4
      check buff.popFirst() == byte(ord('1'))
      await fut
      check buff.len == 4

      result = true

      await buff.close()

    check:
      waitFor(testPushTo()) == true

  test "read with size":
    proc testRead(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.pushTo("12345".toBytes())
      var data = newSeq[byte](3)
      await buff.readExactly(addr data[0], 3)
      check ['1', '2', '3'] == string.fromBytes(data)

      result = true

      await buff.close()

    check:
      waitFor(testRead()) == true

  test "readExactly":
    proc testReadExactly(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.pushTo("12345".toBytes())
      check buff.len == 5
      var data: seq[byte] = newSeq[byte](2)
      await buff.readExactly(addr data[0], 2)
      check string.fromBytes(data) == @['1', '2']

      result = true

      await buff.close()

    check:
      waitFor(testReadExactly()) == true

  test "readExactly raises":
    proc testReadExactly(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.pushTo("123".toBytes())
      var data: seq[byte] = newSeq[byte](5)
      var readFut: Future[void]
      readFut = buff.readExactly(addr data[0], 5)
      await buff.close()

      try:
        await readFut
      except LPStreamIncompleteError, LPStreamEOFError:
        result = true

    check:
      waitFor(testReadExactly()) == true

  test "readOnce":
    proc testReadOnce(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      var data: seq[byte] = newSeq[byte](3)
      let readFut = buff.readOnce(addr data[0], 5)
      await buff.pushTo("123".toBytes())
      check buff.len == 3

      check (await readFut) == 3
      check string.fromBytes(data) == @['1', '2', '3']

      result = true

      await buff.close()

    check:
      waitFor(testReadOnce()) == true

  test "write ptr":
    proc testWritePtr(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} =
        check string.fromBytes(data) == "Hello!"

      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      var data = "Hello!"
      await buff.write(addr data[0], data.len)

      result = true

      await buff.close()

    check:
      waitFor(testWritePtr()) == true

  test "write string":
    proc testWritePtr(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} =
        check string.fromBytes(data) == "Hello!"

      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.write("Hello!")

      result = true

      await buff.close()

    check:
      waitFor(testWritePtr()) == true

  test "write bytes":
    proc testWritePtr(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} =
        check string.fromBytes(data) == "Hello!"

      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.write("Hello!".toBytes())

      result = true

      await buff.close()

    check:
      waitFor(testWritePtr()) == true

  test "write should happen in order":
    proc testWritePtr(): Future[bool] {.async.} =
      var count = 1
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} =
        check string.fromBytes(data) == &"Msg {$count}"
        count.inc

      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.write("Msg 1")
      await buff.write("Msg 2")
      await buff.write("Msg 3")
      await buff.write("Msg 4")
      await buff.write("Msg 5")
      await buff.write("Msg 6")
      await buff.write("Msg 7")
      await buff.write("Msg 8")
      await buff.write("Msg 9")
      await buff.write("Msg 10")

      result = true

      await buff.close()

    check:
      waitFor(testWritePtr()) == true

  test "reads should happen in order":
    proc testWritePtr(): Future[bool] {.async.} =
      proc writeHandler(data: seq[byte]) {.async, gcsafe.} = discard
      let buff = newBufferStream(writeHandler, 10)
      check buff.len == 0

      await buff.pushTo("Msg 1".toBytes())
      await buff.pushTo("Msg 2".toBytes())
      await buff.pushTo("Msg 3".toBytes())

      var data = newSeq[byte](5)
      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 1"

      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 2"

      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 3"

      await buff.pushTo("Msg 4".toBytes())
      await buff.pushTo("Msg 5".toBytes())
      await buff.pushTo("Msg 6".toBytes())

      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 4"

      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 5"

      await buff.readExactly(addr data[0], 5)
      check string.fromBytes(data) == "Msg 6"

      result = true

      await buff.close()

    check:
      waitFor(testWritePtr()) == true

  test "pipe two streams without the `pipe` or `|` helpers":
    proc pipeTest(): Future[bool] {.async.} =
      proc writeHandler1(data: seq[byte]) {.async, gcsafe.}
      proc writeHandler2(data: seq[byte]) {.async, gcsafe.}

      var buf1 = newBufferStream(writeHandler1)
      var buf2 = newBufferStream(writeHandler2)

      proc writeHandler1(data: seq[byte]) {.async, gcsafe.} =
        var msg = string.fromBytes(data)
        check  msg == "Hello!"
        await buf2.pushTo(data)

      proc writeHandler2(data: seq[byte]) {.async, gcsafe.} =
        var msg = string.fromBytes(data)
        check  msg == "Hello!"
        await buf1.pushTo(data)

      var res1: seq[byte] = newSeq[byte](7)
      var readFut1 = buf1.readExactly(addr res1[0], 7)

      var res2: seq[byte] = newSeq[byte](7)
      var readFut2 = buf2.readExactly(addr res2[0], 7)

      await buf1.pushTo("Hello2!".toBytes())
      await buf2.pushTo("Hello1!".toBytes())

      await allFuturesThrowing(readFut1, readFut2)

      check:
        res1 == "Hello2!".toBytes()
        res2 == "Hello1!".toBytes()

      result = true

      await buf1.close()
      await buf2.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe A -> B":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()
      var buf2 = buf1.pipe(newBufferStream())

      var res1: seq[byte] = newSeq[byte](7)
      var readFut = buf2.readExactly(addr res1[0], 7)
      await buf1.write("Hello1!".toBytes())
      await readFut

      check:
        res1 == "Hello1!".toBytes()

      result = true

      await buf1.close()
      await buf2.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe A -> B and B -> A":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()
      var buf2 = newBufferStream()

      buf1 = buf1.pipe(buf2).pipe(buf1)

      var res1: seq[byte] = newSeq[byte](7)
      var readFut1 = buf1.readExactly(addr res1[0], 7)

      var res2: seq[byte] = newSeq[byte](7)
      var readFut2 = buf2.readExactly(addr res2[0], 7)

      await buf1.write("Hello1!".toBytes())
      await buf2.write("Hello2!".toBytes())
      await allFuturesThrowing(readFut1, readFut2)

      check:
        res1 == "Hello2!".toBytes()
        res2 == "Hello1!".toBytes()

      result = true

      await buf1.close()
      await buf2.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe A -> A (echo)":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()

      buf1 = buf1.pipe(buf1)

      proc reader(): Future[seq[byte]] {.async.} =
        result = newSeq[byte](6)
        await buf1.readExactly(addr result[0], 6)

      proc writer(): Future[void] = buf1.write("Hello!".toBytes())

      var writerFut = writer()
      var readerFut = reader()

      await writerFut
      check:
        (await readerFut) == "Hello!".toBytes()

      result = true

      await buf1.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe with `|` operator - A -> B":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()
      var buf2 = buf1 | newBufferStream()

      var res1: seq[byte] = newSeq[byte](7)
      var readFut = buf2.readExactly(addr res1[0], 7)
      await buf1.write("Hello1!".toBytes())
      await readFut

      check:
        res1 == "Hello1!".toBytes()

      result = true

      await buf1.close()
      await buf2.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe with `|` operator - A -> B and B -> A":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()
      var buf2 = newBufferStream()

      buf1 = buf1 | buf2 | buf1

      var res1: seq[byte] = newSeq[byte](7)
      var readFut1 = buf1.readExactly(addr res1[0], 7)

      var res2: seq[byte] = newSeq[byte](7)
      var readFut2 = buf2.readExactly(addr res2[0], 7)

      await buf1.write("Hello1!".toBytes())
      await buf2.write("Hello2!".toBytes())
      await allFuturesThrowing(readFut1, readFut2)

      check:
        res1 == "Hello2!".toBytes()
        res2 == "Hello1!".toBytes()

      result = true

      await buf1.close()
      await buf2.close()

    check:
      waitFor(pipeTest()) == true

  test "pipe with `|` operator - A -> A (echo)":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream()

      buf1 = buf1 | buf1

      proc reader(): Future[seq[byte]] {.async.} =
        result = newSeq[byte](6)
        await buf1.readExactly(addr result[0], 6)

      proc writer(): Future[void] = buf1.write("Hello!".toBytes())

      var writerFut = writer()
      var readerFut = reader()

      await writerFut
      check:
        (await readerFut) == "Hello!".toBytes()

      result = true

      await buf1.close()

    check:
      waitFor(pipeTest()) == true

  # TODO: Need to implement deadlock prevention when
  # piping to self
  test "pipe deadlock":
    proc pipeTest(): Future[bool] {.async.} =
      var buf1 = newBufferStream(size = 5)

      buf1 = buf1 | buf1

      var count = 30000
      proc reader() {.async.} =
        var data = newSeq[byte](7)
        await buf1.readExactly(addr data[0], 7)

      proc writer() {.async.} =
        while count > 0:
          await buf1.write("Hello2!".toBytes())
          count.dec

      var writerFut = writer()
      var readerFut = reader()

      await allFuturesThrowing(readerFut, writerFut)
      result = true

      await buf1.close()

    check:
      waitFor(pipeTest()) == true

  test "shouldn't get stuck on close":
    proc closeTest(): Future[bool] {.async.} =
      proc createMessage(tmplate: string, size: int): seq[byte] =
        result = newSeq[byte](size)
        for i in 0 ..< len(result):
          result[i] = byte(tmplate[i mod len(tmplate)])

      var stream = newBufferStream()
      var message = createMessage("MESSAGE", DefaultBufferSize * 2 + 1)
      var fut = stream.pushTo(message)
      await stream.close()
      try:
        await wait(fut, 100.milliseconds)
        result = true
      except AsyncTimeoutError:
        result = false

      await stream.close()

      check:
        waitFor(closeTest()) == true
