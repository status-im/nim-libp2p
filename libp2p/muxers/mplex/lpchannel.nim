## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[oids, strformat]
import chronos, chronicles, metrics
import ./coder,
       ../muxer,
       nimcrypto/utils,
       ../../stream/[bufferstream, connection, streamseq],
       ../../peerinfo

export connection

logScope:
  topics = "mplexchannel"

## Channel half-closed states
##
## | State    | Closed local      | Closed remote
## |=============================================
## | Read     | Yes (until EOF)   | No
## | Write    | No                | Yes
##
## Channels are considered fully closed when both outgoing and incoming
## directions are closed and when the reader of the channel has read the
## EOF marker

type
  LPChannel* = ref object of BufferStream
    id*: uint64                   # channel id
    name*: string                 # name of the channel (for debugging)
    conn*: Connection             # wrapped connection used to for writing
    initiator*: bool              # initiated remotely or locally flag
    isOpen*: bool                 # has channel been opened
    closedLocal*: bool            # has channel been closed locally
    msgCode*: MessageType         # cached in/out message code
    closeCode*: MessageType       # cached in/out close code
    resetCode*: MessageType       # cached in/out reset code

proc open*(s: LPChannel) {.async, gcsafe.}

func shortLog*(s: LPChannel): auto =
  if s.isNil: "LPChannel(nil)"
  elif s.conn.peerInfo.isNil: $s.oid
  elif s.name != $s.oid and s.name.len > 0:
    &"{shortLog(s.conn.peerInfo.peerId)}:{s.oid}:{s.name}"
  else: &"{shortLog(s.conn.peerInfo.peerId)}:{s.oid}"
chronicles.formatIt(LPChannel): shortLog(it)

proc open*(s: LPChannel) {.async, gcsafe.} =
  trace "Opening channel", s, conn = s.conn
  await s.conn.writeMsg(s.id, MessageType.New, s.name)
  s.isOpen = true

method closed*(s: LPChannel): bool =
  s.closedLocal

proc closeUnderlying(s: LPChannel): Future[void] {.async.} =
  ## Channels may be closed for reading and writing in any order - we'll close
  ## the underlying bufferstream when both directions are closed
  if s.closedLocal and s.isEof:
    await procCall BufferStream(s).close()

proc reset*(s: LPChannel) {.async, gcsafe.} =
  if s.isClosed:
    trace "Already closed", s
    return

  trace "Resetting channel", s, len = s.len

  # First, make sure any new calls to `readOnce` and `pushData` etc will fail -
  # there may already be such calls in the event queue however
  s.closedLocal = true
  s.isEof = true
  s.readBuf = StreamSeq()
  s.pushedEof = true

  for i in 0..<s.pushing:
    # Make sure to drain any ongoing pushes - there's already at least one item
    # more in the queue already so any ongoing reads shouldn't interfere
    # Notably, popFirst is not fair - which reader/writer gets woken up depends
    discard await s.readQueue.popFirst()

  if s.readQueue.len == 0 and s.pushing == 0:
    # There is no push ongoing and nothing on the queue - let's place an
    # EOF marker there so that any reader is woken up - we don't need to
    # synchronize here
    await s.readQueue.addLast(@[])

  if not s.conn.isClosed:
    # If the connection is still active, notify the other end
    proc resetMessage() {.async.} =
      try:
          trace "sending reset message", s, conn = s.conn
          await s.conn.writeMsg(s.id, s.resetCode) # write reset
      except CatchableError as exc:
        # No cancellations, errors handled in writeMsg
        trace "Can't send reset message", s, conn = s.conn, msg = exc.msg

    asyncSpawn resetMessage()

  # This should wake up any readers by pushing an EOF marker at least
  await procCall BufferStream(s).close() # noraises, nocancels

  trace "Channel reset", s

method close*(s: LPChannel) {.async, gcsafe.} =
  ## Close channel for writing - a message will be sent to the other peer
  ## informing them that the channel is closed and that we're waiting for
  ## their acknowledgement.
  if s.closedLocal:
    trace "Already closed", s
    return
  s.closedLocal = true

  trace "Closing channel", s, conn = s.conn, len = s.len

  if s.isOpen:
    try:
      await s.conn.writeMsg(s.id, s.closeCode) # write close
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      # It's harmless that close message cannot be sent - the connection is
      # likely down already
      trace "Cannot send close message", s, id = s.id

  await s.closeUnderlying() # maybe already eofed

  trace "Closed channel", s, len = s.len

method initStream*(s: LPChannel) =
  if s.objName.len == 0:
    s.objName = "LPChannel"

  s.timeoutHandler = proc(): Future[void] {.gcsafe.} =
    trace "Idle timeout expired, resetting LPChannel", s
    s.reset()

  procCall BufferStream(s).initStream()

method readOnce*(s: LPChannel,
                 pbytes: pointer,
                 nbytes: int):
                 Future[int] {.async.} =
  try:
    let bytes = await procCall BufferStream(s).readOnce(pbytes, nbytes)
    trace "readOnce", s, bytes
    if bytes == 0:
      await s.closeUnderlying()
    return bytes
  except CatchableError as exc:
    await s.closeUnderlying()
    raise exc

method write*(s: LPChannel, msg: seq[byte]): Future[void] {.async.} =
  if s.closedLocal or s.conn.closed:
    raise newLPStreamClosedError()

  if msg.len == 0:
    return

  try:
    if not s.isOpen:
      await s.open()

    # writes should happen in sequence
    trace "write msg", s, conn = s.conn, len = msg.len

    await s.conn.writeMsg(s.id, s.msgCode, msg)
    s.activity = true
  except CatchableError as exc:
    trace "exception in lpchannel write handler", s, msg = exc.msg
    await s.conn.close()
    raise exc

proc init*(
  L: type LPChannel,
  id: uint64,
  conn: Connection,
  initiator: bool,
  name: string = "",
  timeout: Duration = DefaultChanTimeout): LPChannel =

  let chann = L(
    id: id,
    name: name,
    conn: conn,
    initiator: initiator,
    timeout: timeout,
    isOpen: if initiator: false else: true,
    msgCode: if initiator: MessageType.MsgOut else: MessageType.MsgIn,
    closeCode: if initiator: MessageType.CloseOut else: MessageType.CloseIn,
    resetCode: if initiator: MessageType.ResetOut else: MessageType.ResetIn,
    dir: if initiator: Direction.Out else: Direction.In)

  chann.initStream()

  when chronicles.enabledLogLevel == LogLevel.TRACE:
    chann.name = if chann.name.len > 0: chann.name else: $chann.oid

  trace "Created new lpchannel", chann, id, initiator

  return chann
