## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import pkg/[chronos, nimcrypto/utils, chronicles, stew/byteutils]
import ../../stream/connection,
       ../../utility,
       ../../varint,
       ../../vbuffer,
       ../muxer

logScope:
  topics = "libp2p mplexcoder"

type
  MessageType* {.pure.} = enum
    New,
    MsgIn,
    MsgOut,
    CloseIn,
    CloseOut,
    ResetIn,
    ResetOut

  Msg* = tuple
    id: uint64
    msgType: MessageType
    data: seq[byte]

  InvalidMplexMsgType* = object of MuxerError

# https://github.com/libp2p/specs/tree/master/mplex#writing-to-a-stream
const MaxMsgSize* = 1 shl 20 # 1mb

proc newInvalidMplexMsgType(): ref InvalidMplexMsgType =
  newException(InvalidMplexMsgType, "invalid message type")

proc readMsg*(conn: Connection): Future[Msg] {.async, gcsafe.} =
  let header = await conn.readVarint()
  trace "read header varint", varint = header, conn

  let data = await conn.readLp(MaxMsgSize)
  trace "read data", dataLen = data.len, data = shortLog(data), conn

  let msgType = header and 0x7
  if msgType.int > ord(MessageType.ResetOut):
    raise newInvalidMplexMsgType()

  return (header shr 3, MessageType(msgType), data)

proc writeMsg*(conn: Connection,
               id: uint64,
               msgType: MessageType,
               data: seq[byte] = @[]): Future[void] =
  var
    left = data.len
    offset = 0
    buf = initVBuffer()

  # Split message into length-prefixed chunks
  while left > 0 or data.len == 0:
    let
      chunkSize = if left > MaxMsgSize: MaxMsgSize - 64 else: left

    buf.writePBVarint(id shl 3 or ord(msgType).uint64)
    buf.writeSeq(data.toOpenArray(offset, offset + chunkSize - 1))
    left = left - chunkSize
    offset = offset + chunkSize

    if data.len == 0:
      break

  trace "writing mplex message",
    conn, id, msgType, data = data.len, encoded = buf.buffer.len

  # Write all chunks in a single write to avoid async races where a close
  # message gets written before some of the chunks
  conn.write(buf.buffer)

proc writeMsg*(conn: Connection,
               id: uint64,
               msgType: MessageType,
               data: string): Future[void] =
  conn.writeMsg(id, msgType, data.toBytes())
