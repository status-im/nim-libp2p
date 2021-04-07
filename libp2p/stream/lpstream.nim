## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/oids
import stew/byteutils
import chronicles, chronos, metrics
import ../varint,
       ../peerinfo,
       ../multiaddress,
       ../errors

export errors

declareGauge(libp2p_open_streams,
  "open stream instances", labels = ["type", "dir"])

export oids

logScope:
  topics = "libp2p lpstream"

const
  LPStreamTrackerName* = "LPStream"
  Eof* = @[]

type
  Direction* {.pure.} = enum
    In, Out

  LPStream* = ref object of RootObj
    closeEvent*: AsyncEvent
    isClosed*: bool
    isEof*: bool
    objName*: string
    oid*: Oid
    dir*: Direction
    closedWithEOF: bool # prevent concurrent calls

  LPStreamError* = object of LPError
  LPStreamIncompleteError* = object of LPStreamError
  LPStreamIncorrectDefect* = object of Defect
  LPStreamLimitError* = object of LPStreamError
  LPStreamReadError* = object of LPStreamError
    par*: ref CatchableError
  LPStreamWriteError* = object of LPStreamError
    par*: ref CatchableError
  LPStreamEOFError* = object of LPStreamError
  LPStreamClosedError* = object of LPStreamError

  InvalidVarintError* = object of LPStreamError
  MaxSizeError* = object of LPStreamError

  StreamTracker* = ref object of TrackerBase
    opened*: uint64
    closed*: uint64

proc setupStreamTracker(name: string): StreamTracker =
  let tracker = new StreamTracker

  proc dumpTracking(): string {.gcsafe.} =
    return "Opened " & tracker.id & ": " & $tracker.opened & "\n" &
            "Closed " & tracker.id & ": " & $tracker.closed

  proc leakTransport(): bool {.gcsafe.} =
    return (tracker.opened != tracker.closed)

  tracker.id = name
  tracker.opened = 0
  tracker.closed = 0
  tracker.dump = dumpTracking
  tracker.isLeaked = leakTransport
  addTracker(name, tracker)

  return tracker

proc getStreamTracker(name: string): StreamTracker {.gcsafe.} =
  result = cast[StreamTracker](getTracker(name))
  if isNil(result):
    result = setupStreamTracker(name)

proc newLPStreamReadError*(p: ref CatchableError): ref CatchableError =
  var w = newException(LPStreamReadError, "Read stream failed")
  w.msg = w.msg & ", originated from [" & $p.name & "] " & p.msg
  w.par = p
  result = w

proc newLPStreamReadError*(msg: string): ref CatchableError =
  newException(LPStreamReadError, msg)

proc newLPStreamWriteError*(p: ref CatchableError): ref CatchableError =
  var w = newException(LPStreamWriteError, "Write stream failed")
  w.msg = w.msg & ", originated from [" & $p.name & "] " & p.msg
  w.par = p
  result = w

proc newLPStreamIncompleteError*(): ref CatchableError =
  result = newException(LPStreamIncompleteError, "Incomplete data received")

proc newLPStreamLimitError*(): ref CatchableError =
  result = newException(LPStreamLimitError, "Buffer limit reached")

proc newLPStreamIncorrectDefect*(m: string): ref Defect =
  result = newException(LPStreamIncorrectDefect, m)

proc newLPStreamEOFError*(): ref CatchableError =
  result = newException(LPStreamEOFError, "Stream EOF!")

proc newLPStreamClosedError*(): ref Exception =
  result = newException(LPStreamClosedError, "Stream Closed!")

func shortLog*(s: LPStream): auto =
  if s.isNil: "LPStream(nil)"
  else: $s.oid
chronicles.formatIt(LPStream): shortLog(it)

method initStream*(s: LPStream) {.base.} =
  if s.objName.len == 0:
    s.objName = "LPStream"

  s.closeEvent = newAsyncEvent()
  s.oid = genOid()

  libp2p_open_streams.inc(labelValues = [s.objName, $s.dir])
  inc getStreamTracker(s.objName).opened
  trace "Stream created", s, objName = s.objName, dir = $s.dir

proc join*(s: LPStream): Future[void] =
  s.closeEvent.wait()

method closed*(s: LPStream): bool {.base, raises: [Defect].} =
  s.isClosed

method atEof*(s: LPStream): bool {.base, raises: [Defect].} =
  s.isEof

method readOnce*(s: LPStream,
                 pbytes: pointer,
                 nbytes: int):
                 Future[int]
  {.base, async.} =
  doAssert(false, "not implemented!")

proc readExactly*(s: LPStream,
                  pbytes: pointer,
                  nbytes: int):
                  Future[void] {.async.} =
  if s.atEof:
    raise newLPStreamEOFError()

  if nbytes == 0:
    return

  logScope:
    s
    nbytes = nbytes
    objName = s.objName

  var pbuffer = cast[ptr UncheckedArray[byte]](pbytes)
  var read = 0
  while read < nbytes and not(s.atEof()):
    read += await s.readOnce(addr pbuffer[read], nbytes - read)

  if read == 0:
    doAssert s.atEof()
    trace "couldn't read all bytes, stream EOF", s, nbytes, read
    raise newLPStreamEOFError()

  if read < nbytes:
    trace "couldn't read all bytes, incomplete data", s, nbytes, read
    raise newLPStreamIncompleteError()

proc readLine*(s: LPStream,
               limit = 0,
               sep = "\r\n"): Future[string]
               {.async, deprecated: "todo".} =
  # TODO replace with something that exploits buffering better
  var lim = if limit <= 0: -1 else: limit
  var state = 0

  while true:
    var ch: char
    await readExactly(s, addr ch, 1)

    if sep[state] == ch:
      inc(state)
      if state == len(sep):
        break
    else:
      state = 0
      if limit > 0:
        let missing = min(state, lim - len(result) - 1)
        result.add(sep[0 ..< missing])
      else:
        result.add(sep[0 ..< state])

      result.add(ch)
      if len(result) == lim:
        break

proc readVarint*(conn: LPStream): Future[uint64] {.async, gcsafe.} =
  var
    varint: uint64
    length: int
    buffer: array[10, byte]

  for i in 0..<len(buffer):
    await conn.readExactly(addr buffer[i], 1)
    let res = PB.getUVarint(buffer.toOpenArray(0, i), length, varint)
    if res.isOk():
      return varint
    if res.error() != VarintError.Incomplete:
      break
  if true: # can't end with a raise apparently
    raise (ref InvalidVarintError)(msg: "Cannot parse varint")

proc readLp*(s: LPStream, maxSize: int): Future[seq[byte]] {.async, gcsafe.} =
  ## read length prefixed msg, with the length encoded as a varint
  let
    length = await s.readVarint()
    maxLen = uint64(if maxSize < 0: int.high else: maxSize)

  if length > maxLen:
    raise (ref MaxSizeError)(msg: "Message exceeds maximum length")

  if length == 0:
    return

  var res = newSeq[byte](length)
  await s.readExactly(addr res[0], res.len)
  return res

method write*(s: LPStream, msg: seq[byte]): Future[void] {.base.} =
  doAssert(false, "not implemented!")

proc writeLp*(s: LPStream, msg: openArray[byte]): Future[void] =
  ## Write `msg` with a varint-encoded length prefix
  let vbytes = PB.toBytes(msg.len().uint64)
  var buf = newSeqUninitialized[byte](msg.len() + vbytes.len)
  buf[0..<vbytes.len] = vbytes.toOpenArray()
  buf[vbytes.len..<buf.len] = msg
  s.write(buf)

proc writeLp*(s: LPStream, msg: string): Future[void] =
  writeLp(s, msg.toOpenArrayByte(0, msg.high))

proc write*(s: LPStream, pbytes: pointer, nbytes: int): Future[void] {.deprecated: "seq".} =
  s.write(@(toOpenArray(cast[ptr UncheckedArray[byte]](pbytes), 0, nbytes - 1)))

proc write*(s: LPStream, msg: string): Future[void] =
  s.write(msg.toBytes())

method closeImpl*(s: LPStream): Future[void] {.async, base.} =
  ## Implementation of close - called only once
  trace "Closing stream", s, objName = s.objName, dir = $s.dir
  s.closeEvent.fire()
  libp2p_open_streams.dec(labelValues = [s.objName, $s.dir])
  inc getStreamTracker(s.objName).closed
  trace "Closed stream", s, objName = s.objName, dir = $s.dir

method close*(s: LPStream): Future[void] {.base, async.} = # {.raises [Defect].}
  ## close the stream - this may block, but will not raise exceptions
  ##
  if s.isClosed:
    trace "Already closed", s
    return

  s.isClosed = true # Set flag before performing virtual close

  # An separate implementation method is used so that even when derived types
  # override `closeImpl`, it is called only once - anyone overriding `close`
  # itself must implement this - once-only check as well, with their own field
  await closeImpl(s)

proc closeWithEOF*(s: LPStream): Future[void] {.async.} =
  ## Close the stream and wait for EOF - use this with half-closed streams where
  ## an EOF is expected to arrive from the other end.
  ##
  ## Note - this should only be used when there has been an in-protocol
  ## notification that no more data will arrive and that the only thing left
  ## for the other end to do is to close the stream gracefully.
  ##
  ## In particular, it must not be used when there is another concurrent read
  ## ongoing (which may be the case during cancellations)!
  ##

  trace "Closing with EOF", s
  if s.closedWithEOF:
    trace "Already closed"
    return

  # prevent any further calls to avoid triggering
  # reading the stream twice (which should assert)
  s.closedWithEOF = true
  await s.close()

  if s.atEof():
    return

  try:
    var buf: array[8, byte]
    if (await readOnce(s, addr buf[0], buf.len)) != 0:
      debug "Unexpected bytes while waiting for EOF", s
  except LPStreamEOFError:
    trace "Expected EOF came", s
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "Unexpected error while waiting for EOF", s, msg = exc.msg
