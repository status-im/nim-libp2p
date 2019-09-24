## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos, chronicles
import ../protocols/protocol,
       ../connection

logScope:
  topic = "Muxer"

type
  StreamHandler* = proc(conn: Connection): Future[void] {.gcsafe.}
  MuxerHandler* = proc(muxer: Muxer): Future[void] {.gcsafe.}

  Muxer* = ref object of RootObj
    streamHandler*: StreamHandler
    connection*: Connection

  MuxerCreator* = proc(conn: Connection): Muxer {.gcsafe, closure.}
  # this wraps a creator proc that knows how to make muxers
  MuxerProvider* = ref object of LPProtocol
    newMuxer*: MuxerCreator
    streamHandler*: StreamHandler
    muxerHandler*: MuxerHandler

method newStream*(m: Muxer, name: string = ""): Future[Connection] {.base, async, gcsafe.} = discard
method close*(m: Muxer) {.base, async, gcsafe.} = discard
method handle*(m: Muxer): Future[void] {.base, async, gcsafe.} = discard
method `streamHandler=`*(m: Muxer, handler: StreamHandler) {.base, gcsafe.} = 
  m.streamHandler = handler

proc newMuxerProvider*(creator: MuxerCreator, codec: string): MuxerProvider {.gcsafe.} = 
  new result
  result.newMuxer = creator
  result.codec = codec
  result.init()

method `streamHandler=`*(m: MuxerProvider, handler: StreamHandler) {.base, gcsafe.} = 
  ## new stream (channels) handler
  ##
  ## triggered every time there is a new 
  ## stream (channel) oppened over a muxed 
  ## connection
  ##
  m.streamHandler = handler

method `muxerHandler=`*(m: MuxerProvider, handler: MuxerHandler) {.base, gcsafe.} = 
  ## new muxer (muxed connections) handler
  ##
  ## triggered every time there is a new muxed 
  ## connection created
  ##
  m.muxerHandler = handler

method init(c: MuxerProvider) =
  proc handler(conn: Connection, proto: string) {.async, gcsafe, closure.} =
    let muxer = c.newMuxer(conn)
    if not isNil(c.muxerHandler):
      asyncCheck c.muxerHandler(muxer)

    if not isNil(c.streamHandler):
      muxer.streamHandler = c.streamHandler

    await muxer.handle()

  c.handler = handler
