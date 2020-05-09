## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos

# https://github.com/libp2p/specs/tree/master/mplex#writing-to-a-stream
const MaxMsgSize* = 1 shl 20 # 1mb
const MaxChannels* = 1000
const MplexCodec* = "/mplex/6.7.0"
const MaxReadWriteTime* = 5.seconds

type
  MplexNoSuchChannel* = object of CatchableError

  MessageType* {.pure.} = enum
    New,
    MsgIn,
    MsgOut,
    CloseIn,
    CloseOut,
    ResetIn,
    ResetOut
