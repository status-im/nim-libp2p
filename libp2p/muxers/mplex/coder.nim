## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos
import nimcrypto/utils, chronicles, stew/byteutils
import types,
       ../../stream/connection,
       ../../utility,
       ../../varint,
       ../../vbuffer

logScope:
  topics = "mplexcoder"

type
  Msg* = tuple
    id: uint64
    msgType: MessageType
    data: seq[byte]

  InvalidMplexMsgType = object of CatchableError

proc newInvalidMplexMsgType*(): ref InvalidMplexMsgType =
  newException(InvalidMplexMsgType, "invalid message type")

proc readMsg*(conn: Connection): Future[Msg] {.async, gcsafe.} =
  let header = await conn.readVarint()
  trace "read header varint", varint = header

  let data = await conn.readLp(MaxMsgSize)
  trace "read data", dataLen = data.len, data = shortLog(data)

  let msgType = header and 0x7
  if msgType.int > ord(MessageType.ResetOut):
    raise newInvalidMplexMsgType()

  result = (header shr 3, MessageType(msgType), data)

proc writeMsg*(conn: Connection,
               id: uint64,
               msgType: MessageType,
               data: seq[byte] = @[]) {.async, gcsafe.} =
  trace "sending data over mplex", id,
                                   msgType,
                                   data = data.len
  var
    left = data.len
    offset = 0
  while left > 0 or data.len == 0:
    let
      chunkSize = if left > MaxMsgSize: MaxMsgSize - 64 else: left
      chunk = if chunkSize > 0 : data[offset..(offset + chunkSize - 1)] else: data
    ## write length prefixed
    var buf = initVBuffer()
    buf.writePBVarint(id shl 3 or ord(msgType).uint64)
    buf.writePBVarint(chunkSize.uint64) # size should be always sent
    buf.finish()
    left = left - chunkSize
    offset = offset + chunkSize
    await conn.write(buf.buffer & chunk)

    if data.len == 0:
      return

proc writeMsg*(conn: Connection,
               id: uint64,
               msgType: MessageType,
               data: string) {.async, gcsafe.} =
  # TODO: changing this to
  #`await conn.writeMsg(id, msgType, data.toBytes())`
  # causes all sorts of race conditions and hangs.
  # DON'T DO IT!
  result = conn.writeMsg(id, msgType, data.toBytes())
