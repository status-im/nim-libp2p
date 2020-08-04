## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import oids
import chronicles, chronos, metrics
import ../varint,
       ../vbuffer,
       ../peerinfo,
       ../multiaddress

declareGauge(libp2p_open_streams, "open stream instances", labels = ["type"])

logScope:
  topics = "lpstream"

type
  LPStream* = ref object of RootObj
    closeEvent*: AsyncEvent
    isClosed*: bool
    isEof*: bool
    objName*: string
    oid*: Oid

  LPStreamError* = object of CatchableError
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

method initStream*(s: LPStream) {.base.} =
  if s.objName.len == 0:
    s.objName = "LPStream"

  s.oid = genOid()
  libp2p_open_streams.inc(labelValues = [s.objName])
  trace "stream created", oid = $s.oid, name = s.objName

proc join*(s: LPStream): Future[void] =
  s.closeEvent.wait()

method closed*(s: LPStream): bool {.base, inline.} =
  s.isClosed

method atEof*(s: LPStream): bool {.base, inline.} =
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

  logScope:
    nbytes = nbytes
    obName = s.objName
    stack = getStackTrace()
    oid = $s.oid

  var pbuffer = cast[ptr UncheckedArray[byte]](pbytes)
  var read = 0
  while read < nbytes and not(s.atEof()):
    read += await s.readOnce(addr pbuffer[read], nbytes - read)

  if read < nbytes:
    trace "incomplete data received", read
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

proc writeLp*(s: LPStream, msg: string | seq[byte]): Future[void] {.gcsafe.} =
  ## write length prefixed
  var buf = initVBuffer()
  buf.writeSeq(msg)
  buf.finish()
  s.write(buf.buffer)

method write*(s: LPStream, msg: seq[byte]) {.base, async.} =
  doAssert(false, "not implemented!")

proc write*(s: LPStream, pbytes: pointer, nbytes: int): Future[void] {.deprecated: "seq".} =
  s.write(@(toOpenArray(cast[ptr UncheckedArray[byte]](pbytes), 0, nbytes - 1)))

proc write*(s: LPStream, msg: string): Future[void] =
  s.write(@(toOpenArrayByte(msg, 0, msg.high)))

# TODO: split `close` into `close` and `dispose/destroy`
method close*(s: LPStream) {.base, async.} =
  if not s.isClosed:
    s.isClosed = true
    s.closeEvent.fire()
    libp2p_open_streams.dec(labelValues = [s.objName])
    trace "stream destroyed", oid = $s.oid, name = s.objName
