## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

## This module implements an asynchronous buffer stream
## which emulates physical async IO.
##
## The stream is based on the standard library's `Deque`,
## which is itself based on a ring buffer.
##
## It works by exposing a regular LPStream interface and
## a method ``pushTo`` to push data to the internal read
## buffer; as well as a handler that can be registrered
## that gets triggered on every write to the stream. This
## allows using the buffered stream as a sort of proxy,
## which can be consumed as a regular LPStream but allows
## injecting data for reads and intercepting writes.
##
## Another notable feature is that the stream is fully
## ordered and asynchronous. Reads are queued up in order
## and are suspended when not enough data available. This
## allows preserving backpressure while maintaining full
## asynchrony. Both writting to the internal buffer with
## ``pushTo`` as well as reading with ``read*` methods,
## will suspend until either the amount of elements in the
## buffer goes below ``maxSize`` or more data becomes available.

import deques, math, oids
import chronos, chronicles
import ../stream/lpstream

const DefaultBufferSize* = 1024

type
  # TODO: figure out how to make this generic to avoid casts
  WriteHandler* = proc (data: seq[byte]): Future[void] {.gcsafe.}

  BufferStream* = ref object of LPStream
    maxSize*: int # buffer's max size in bytes
    readBuf: Deque[byte] # this is a ring buffer based dequeue, this makes it perfect as the backing store here
    readReqs: Deque[Future[void]] # use dequeue to fire reads in order
    dataReadEvent: AsyncEvent
    writeHandler*: WriteHandler
    lock: AsyncLock
    isPiped: bool

  AlreadyPipedError* = object of CatchableError
  NotWritableError* = object of CatchableError

proc newAlreadyPipedError*(): ref Exception {.inline.} =
  result = newException(AlreadyPipedError, "stream already piped")

proc newNotWritableError*(): ref Exception {.inline.} =
  result = newException(NotWritableError, "stream is not writable")

proc requestReadBytes(s: BufferStream): Future[void] =
  ## create a future that will complete when more
  ## data becomes available in the read buffer
  result = newFuture[void]()
  s.readReqs.addLast(result)
  trace "requestReadBytes(): added a future to readReqs"

proc initBufferStream*(s: BufferStream,
                       handler: WriteHandler = nil,
                       size: int = DefaultBufferSize) =
  s.maxSize = if isPowerOfTwo(size): size else: nextPowerOfTwo(size)
  s.readBuf = initDeque[byte](s.maxSize)
  s.readReqs = initDeque[Future[void]]()
  s.dataReadEvent = newAsyncEvent()
  s.lock = newAsyncLock()
  s.writeHandler = handler
  s.closeEvent = newAsyncEvent()
  s.oid = genOid()

proc newBufferStream*(handler: WriteHandler = nil,
                      size: int = DefaultBufferSize): BufferStream =
  new result
  result.initBufferStream(handler, size)

proc popFirst*(s: BufferStream): byte =
  result = s.readBuf.popFirst()
  s.dataReadEvent.fire()

proc popLast*(s: BufferStream): byte =
  result = s.readBuf.popLast()
  s.dataReadEvent.fire()

proc shrink(s: BufferStream, fromFirst = 0, fromLast = 0) =
  s.readBuf.shrink(fromFirst, fromLast)
  s.dataReadEvent.fire()

proc len*(s: BufferStream): int = s.readBuf.len

proc pushTo*(s: BufferStream, data: seq[byte]) {.async.} =
  ## Write bytes to internal read buffer, use this to fill up the
  ## buffer with data.
  ##
  ## This method is async and will wait until  all data has been
  ## written to the internal buffer; this is done so that backpressure
  ## is preserved.
  ##

  logScope:
    stream_oid = $s.oid

  try:
    await s.lock.acquire()
    var index = 0
    while not s.closed():
      while index < data.len and s.readBuf.len < s.maxSize:
        s.readBuf.addLast(data[index])
        inc(index)
      trace "pushTo()", msg = "added " & $index & " bytes to readBuf"

      # resolve the next queued read request
      if s.readReqs.len > 0:
        s.readReqs.popFirst().complete()
        trace "pushTo(): completed a readReqs future"

      if index >= data.len:
        return

      # if we couldn't transfer all the data to the
      # internal buf wait on a read event
      await s.dataReadEvent.wait()
      s.dataReadEvent.clear()
  finally:
    s.lock.release()

method read*(s: BufferStream, n = -1): Future[seq[byte]] {.async.} =
  ## Read all bytes (n <= 0) or exactly `n` bytes from buffer
  ##
  ## This procedure allocates buffer seq[byte] and return it as result.
  ##
  logScope:
    stream_oid = $s.oid

  trace "read()", requested_bytes = n
  var size = if n > 0: n else: s.readBuf.len()
  var index = 0

  if s.readBuf.len() == 0:
    await s.requestReadBytes()

  while index < size:
    while s.readBuf.len() > 0 and index < size:
      result.add(s.popFirst())
      inc(index)
    trace "read()", read_bytes = index

    if index < size:
      await s.requestReadBytes()

method readExactly*(s: BufferStream,
                    pbytes: pointer,
                    nbytes: int):
                    Future[void] {.async.} =
  ## Read exactly ``nbytes`` bytes from read-only stream ``rstream`` and store
  ## it to ``pbytes``.
  ##
  ## If EOF is received and ``nbytes`` is not yet read, the procedure
  ## will raise ``LPStreamIncompleteError``.
  ##
  logScope:
    stream_oid = $s.oid

  var buff: seq[byte]
  try:
    buff = await s.read(nbytes)
  except LPStreamEOFError as exc:
    trace "Exception occured", exc = exc.msg

  if nbytes > buff.len():
    raise newLPStreamIncompleteError()

  copyMem(pbytes, addr buff[0], nbytes)

method readLine*(s: BufferStream,
                 limit = 0,
                 sep = "\r\n"):
                 Future[string] {.async.} =
  ## Read one line from read-only stream ``rstream``, where ``"line"`` is a
  ## sequence of bytes ending with ``sep`` (default is ``"\r\n"``).
  ##
  ## If EOF is received, and ``sep`` was not found, the method will return the
  ## partial read bytes.
  ##
  ## If the EOF was received and the internal buffer is empty, return an
  ## empty string.
  ##
  ## If ``limit`` more then 0, then result string will be limited to ``limit``
  ## bytes.
  ##
  result = ""
  var lim = if limit <= 0: -1 else: limit
  var state = 0
  var index = 0

  index = 0
  while index < s.readBuf.len:
    let ch = char(s.readBuf[index])
    if sep[state] == ch:
      inc(state)
      if state == len(sep):
        s.shrink(index + 1)
        break
    else:
      state = 0
      result.add(ch)
      if len(result) == lim:
        s.shrink(index + 1)
        break
    inc(index)

method readOnce*(s: BufferStream,
                 pbytes: pointer,
                 nbytes: int):
                 Future[int] {.async.} =
  ## Perform one read operation on read-only stream ``rstream``.
  ##
  ## If internal buffer is not empty, ``nbytes`` bytes will be transferred from
  ## internal buffer, otherwise it will wait until some bytes will be received.
  ##
  if s.readBuf.len == 0:
    await s.requestReadBytes()

  var len = if nbytes > s.readBuf.len: s.readBuf.len else: nbytes
  await s.readExactly(pbytes, len)
  result = len

method readUntil*(s: BufferStream,
                  pbytes: pointer,
                  nbytes: int,
                  sep: seq[byte]):
                  Future[int] {.async.} =
  ## Read data from the read-only stream ``rstream`` until separator ``sep`` is
  ## found.
  ##
  ## On success, the data and separator will be removed from the internal
  ## buffer (consumed). Returned data will include the separator at the end.
  ##
  ## If EOF is received, and `sep` was not found, procedure will raise
  ## ``LPStreamIncompleteError``.
  ##
  ## If ``nbytes`` bytes has been received and `sep` was not found, procedure
  ## will raise ``LPStreamLimitError``.
  ##
  ## Procedure returns actual number of bytes read.
  ##
  var
    dest = cast[ptr UncheckedArray[byte]](pbytes)
    state = 0
    k = 0

  let datalen = s.readBuf.len()
  if datalen == 0 and s.readBuf.len() == 0:
    raise newLPStreamIncompleteError()

  var index = 0
  while index < datalen:
    let ch = s.readBuf[index]
    if sep[state] == ch:
      inc(state)
    else:
      state = 0
    if k < nbytes:
      dest[k] = ch
      inc(k)
    else:
      raise newLPStreamLimitError()
    if state == len(sep):
      break
    inc(index)

  if state == len(sep):
    s.shrink(index + 1)
    result = k
  else:
    s.shrink(datalen)

method write*(s: BufferStream,
              pbytes: pointer,
              nbytes: int): Future[void] =
  ## Consume (discard) all bytes (n <= 0) or ``n`` bytes from read-only stream
  ## ``rstream``.
  ##
  ## Return number of bytes actually consumed (discarded).
  ##
  if isNil(s.writeHandler):
    var retFuture = newFuture[void]("BufferStream.write(pointer)")
    retFuture.fail(newNotWritableError())
    return retFuture

  var buf: seq[byte] = newSeq[byte](nbytes)
  copyMem(addr buf[0], pbytes, nbytes)
  result = s.writeHandler(buf)

method write*(s: BufferStream,
              msg: string,
              msglen = -1): Future[void] =
  ## Write string ``sbytes`` of length ``msglen`` to writer stream ``wstream``.
  ##
  ## String ``sbytes`` must not be zero-length.
  ##
  ## If ``msglen < 0`` whole string ``sbytes`` will be writen to stream.
  ## If ``msglen > len(sbytes)`` only ``len(sbytes)`` bytes will be written to
  ## stream.
  ##
  if isNil(s.writeHandler):
    var retFuture = newFuture[void]("BufferStream.write(string)")
    retFuture.fail(newNotWritableError())
    return retFuture

  var buf = ""
  shallowCopy(buf, if msglen > 0: msg[0..<msglen] else: msg)
  result = s.writeHandler(cast[seq[byte]](buf))

method write*(s: BufferStream,
              msg: seq[byte],
              msglen = -1): Future[void] =
  ## Write sequence of bytes ``sbytes`` of length ``msglen`` to writer
  ## stream ``wstream``.
  ##
  ## Sequence of bytes ``sbytes`` must not be zero-length.
  ##
  ## If ``msglen < 0`` whole sequence ``sbytes`` will be writen to stream.
  ## If ``msglen > len(sbytes)`` only ``len(sbytes)`` bytes will be written to
  ## stream.
  ##
  if isNil(s.writeHandler):
    var retFuture = newFuture[void]("BufferStream.write(seq)")
    retFuture.fail(newNotWritableError())
    return retFuture

  var buf: seq[byte]
  shallowCopy(buf, if msglen > 0: msg[0..<msglen] else: msg)
  result = s.writeHandler(buf)

proc pipe*(s: BufferStream,
           target: BufferStream): BufferStream =
  ## pipe the write end of this stream to
  ## be the source of the target stream
  ##
  ## Note that this only works with the LPStream
  ## interface methods `read*` and `write` are
  ## piped.
  ##
  if s.isPiped:
    raise newAlreadyPipedError()

  s.isPiped = true
  let oldHandler = target.writeHandler
  proc handler(data: seq[byte]) {.async, closure, gcsafe.} =
    if not isNil(oldHandler):
      await oldHandler(data)

    # if we're piping to self,
    # then add the data to the
    # buffer directly and fire
    # the read event
    if s == target:
      for b in data:
        s.readBuf.addLast(b)

      # notify main loop of available
      # data
      s.dataReadEvent.fire()
    else:
      await target.pushTo(data)

  s.writeHandler = handler
  result = target

proc `|`*(s: BufferStream, target: BufferStream): BufferStream =
  ## pipe operator to make piping less verbose
  pipe(s, target)

method close*(s: BufferStream) {.async.} =
  ## close the stream and clear the buffer
  trace "closing bufferstream"
  for r in s.readReqs:
    if not(isNil(r)) and not(r.finished()):
      r.fail(newLPStreamEOFError())
  s.dataReadEvent.fire()
  s.readBuf.clear()
  s.closeEvent.fire()
  s.isClosed = true
