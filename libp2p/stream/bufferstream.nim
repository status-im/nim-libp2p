## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/strformat
import stew/byteutils
import chronos, chronicles, metrics
import ../stream/connection
import ./streamseq

when chronicles.enabledLogLevel == LogLevel.TRACE:
  import oids

export connection

logScope:
  topics = "libp2p bufferstream"

const
  BufferStreamTrackerName* = "BufferStream"

type
  BufferStream* = ref object of Connection
    readQueue*: AsyncQueue[seq[byte]] # read queue for managing backpressure
    readBuf*: StreamSeq               # overflow buffer for readOnce
    pushing*: bool                    # number of ongoing push operations
    reading*: bool                    # is there an ongoing read? (only allow one)
    pushedEof*: bool                  # eof marker has been put on readQueue
    returnedEof*: bool                # 0-byte readOnce has been completed

func shortLog*(s: BufferStream): auto =
  try:
    if s.isNil: "BufferStream(nil)"
    elif s.peerInfo.isNil: $s.oid
    else: &"{shortLog(s.peerInfo.peerId)}:{s.oid}"
  except ValueError as exc:
    raiseAssert(exc.msg)

chronicles.formatIt(BufferStream): shortLog(it)

proc len*(s: BufferStream): int =
  s.readBuf.len + (if s.readQueue.len > 0: s.readQueue[0].len() else: 0)

method initStream*(s: BufferStream) =
  if s.objName.len == 0:
    s.objName = "BufferStream"

  procCall Connection(s).initStream()

  s.readQueue = newAsyncQueue[seq[byte]](1)

  trace "BufferStream created", s

proc newBufferStream*(timeout: Duration = DefaultConnectionTimeout): BufferStream =
  new result
  result.timeout = timeout
  result.initStream()

method pushData*(s: BufferStream, data: seq[byte]) {.base, async.} =
  ## Write bytes to internal read buffer, use this to fill up the
  ## buffer with data.
  ##
  ## `pushTo` will block if the queue is full, thus maintaining backpressure.
  ##

  doAssert(not s.pushing,
    &"Only one concurrent push allowed for stream {s.shortLog()}")

  if s.isClosed or s.pushedEof:
    raise newLPStreamEOFError()

  if data.len == 0:
    return # Don't push 0-length buffers, these signal EOF

  # We will block here if there is already data queued, until it has been
  # processed
  try:
    s.pushing = true
    trace "Pushing data", s, data = data.len
    await s.readQueue.addLast(data)
  finally:
    s.pushing = false

method pushEof*(s: BufferStream) {.base, async.} =
  if s.pushedEof:
    return

  doAssert(not s.pushing,
    &"Only one concurrent push allowed for stream {s.shortLog()}")

  s.pushedEof = true

  # We will block here if there is already data queued, until it has been
  # processed
  try:
    s.pushing = true
    trace "Pushing EOF", s
    await s.readQueue.addLast(Eof)
  finally:
    s.pushing = false

method atEof*(s: BufferStream): bool {.raises: [Defect].} =
  s.isEof and s.readBuf.len == 0

method readOnce*(s: BufferStream,
                 pbytes: pointer,
                 nbytes: int):
                 Future[int] {.async.} =
  doAssert(nbytes > 0, "nbytes must be positive integer")
  doAssert(not s.reading,
    &"Only one concurrent read allowed for stream {s.shortLog()}")

  if s.returnedEof:
    raise newLPStreamEOFError()

  var
    p = cast[ptr UncheckedArray[byte]](pbytes)

  # First consume leftovers from previous read
  var rbytes = s.readBuf.consumeTo(toOpenArray(p, 0, nbytes - 1))

  if rbytes < nbytes and not s.isEof:
    # There's space in the buffer - consume some data from the read queue
    s.reading = true
    let buf =
      try:
        await s.readQueue.popFirst()
      except CatchableError as exc:
        # When an exception happens here, the Bufferstream is effectively
        # broken and no more reads will be valid - for now, return EOF if it's
        # called again, though this is not completely true - EOF represents an
        # "orderly" shutdown and that's not what happened here..
        s.returnedEof = true
        raise exc
      finally:
        s.reading = false

    if buf.len == 0:
      # No more data will arrive on read queue
      trace "EOF", s
      s.isEof = true
    else:
      let remaining = min(buf.len, nbytes - rbytes)
      toOpenArray(p, rbytes, nbytes - 1)[0..<remaining] =
        buf.toOpenArray(0, remaining - 1)
      rbytes += remaining

      if remaining < buf.len:
        trace "add leftovers", s, len = buf.len - remaining
        s.readBuf.add(buf.toOpenArray(remaining, buf.high))

  if s.isEof and s.readBuf.len() == 0:
    # We can clear the readBuf memory since it won't be used any more
    s.readBuf = StreamSeq()

  s.activity = true

  # We want to return 0 exactly once - after that, we'll start raising instead -
  # this is a bit nuts in a mixed exception / return value world, but allows the
  # consumer of the stream to rely on the 0-byte read as a "regular" EOF marker
  # (instead of _sometimes_ getting an exception).
  s.returnedEof = rbytes == 0

  return rbytes

method closeImpl*(s: BufferStream): Future[void] =
  ## close the stream and clear the buffer
  trace "Closing BufferStream", s, len = s.len

  # First, make sure any new calls to `readOnce` and `pushData` etc will fail -
  # there may already be such calls in the event queue however
  s.isEof = true
  s.readBuf = StreamSeq()
  s.pushedEof = true

  # Essentially we need to handle the following cases
  #
  # - If a push was in progress but no reader is
  # attached we need to pop the queue
  # - If a read was in progress without without a
  # push/data we need to push the Eof marker to
  # notify the reader that the channel closed
  #
  # In all other cases, there should be a data to complete
  # a read or enough room in the queue/buffer to complete a
  # push.
  #
  # State       | Q Empty  | Q Full
  # ------------|----------|-------
  # Reading     | Push Eof | Na
  # Pushing     | Na       | Pop
  try:
    if not(s.reading and s.pushing):
      if s.reading:
        if s.readQueue.empty():
          # There is an active reader
          s.readQueue.addLastNoWait(Eof)
      elif s.pushing:
        if not s.readQueue.empty():
          discard s.readQueue.popFirstNoWait()
  except AsyncQueueFullError as exc:
    raiseAssert("Fatal, could not pop from read queue")
  except AsyncQueueEmptyError as exc:
    raiseAssert("Fatal, could not push to read queue")

  trace "Closed BufferStream", s

  procCall Connection(s).closeImpl() # noraises, nocancels
