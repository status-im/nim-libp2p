## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

## TODO:
## Timeouts and message limits are still missing
## they need to be added ASAP

import tables, sequtils, options
import chronos, chronicles
import ../muxer,
       ../../connection,
       ../../stream/lpstream,
       coder,
       types,
       lpchannel

logScope:
  topic = "Mplex"

type
  Mplex* = ref object of Muxer
    remote*: Table[uint, LPChannel]
    local*: Table[uint, LPChannel]
    currentId*: uint
    maxChannels*: uint

proc getChannelList(m: Mplex, initiator: bool): var Table[uint, LPChannel] =
  if initiator:
    trace "picking local channels", initiator = initiator
    result = m.local
  else:
    trace "picking remote channels", initiator = initiator
    result = m.remote

proc newStreamInternal*(m: Mplex,
                        initiator: bool = true,
                        chanId: uint = 0,
                        name: string = ""):
                        Future[LPChannel] {.async, gcsafe.} =
  ## create new channel/stream
  let id = if initiator: m.currentId.inc(); m.currentId else: chanId
  trace "creating new channel", channelId = id, initiator = initiator
  result = newChannel(id, m.connection, initiator, name)
  m.getChannelList(initiator)[id] = result

proc cleanupChann(m: Mplex, chann: LPChannel, initiator: bool) {.async, inline.} =
  ## call the channel's `close` to signal the
  ## remote that the channel is closing
  if not isNil(chann) and not chann.closed:
    await chann.close()
    await chann.cleanUp()
    m.getChannelList(initiator).del(chann.id)
    trace "cleaned up channel", id = chann.id

proc messageTimeout(t: Duration): Future[Option[Msg]] {.async, inline.} =
  await sleepAsync(t)
  return Msg.none

method handle*(m: Mplex) {.async, gcsafe.} =
  trace "starting mplex main loop"
  try:
    while not m.connection.closed:
      trace "waiting for data"
      let
        res = await one(m.connection.readMsg(), messageTimeout(MaxReadWriteTime))
        msg = res.read()
      if msg.isNone:
        trace "connection EOF"
        await sleepAsync(1.millis)
        continue

      let (id, msgType, data) = msg.get()
      let initiator = bool(ord(msgType) and 1)
      var channel: LPChannel
      if MessageType(msgType) != MessageType.New:
        let channels = m.getChannelList(initiator)
        if id notin channels:
          trace "Channel not found, skipping", id = id,
                                               initiator = initiator,
                                               msg = msgType
          await sleepAsync(1.millis)
          continue
        channel = channels[id]

      case msgType:
        of MessageType.New:
          let name = cast[string](data)
          channel = await m.newStreamInternal(false, id, name)
          trace "created channel", id = id, name = name, inititator = true
          if not isNil(m.streamHandler):
            let stream = newConnection(channel)
            stream.peerInfo = m.connection.peerInfo

            # cleanup channel once handler is finished
            # stream.closeEvent.wait().addCallback(
            #   proc(udata: pointer) =
            #       asyncCheck cleanupChann(m, channel, initiator))

            asyncCheck m.streamHandler(stream)

            continue
        of MessageType.MsgIn, MessageType.MsgOut:
          trace "pushing data to channel", id = id,
                                           initiator = initiator,
                                           msgType = msgType,
                                           size = data.len

          if data.len > MaxMsgSize:
            raise newLPStreamLimitError();
          await channel.pushTo(data)
        of MessageType.CloseIn, MessageType.CloseOut:
          trace "closing channel", id = id,
                                   initiator = initiator,
                                   msgType = msgType

          await channel.closedByRemote()
          # m.getChannelList(initiator).del(id)
        of MessageType.ResetIn, MessageType.ResetOut:
          trace "resetting channel", id = id,
                                     initiator = initiator,
                                     msgType = msgType

          await channel.resetByRemote()
          m.getChannelList(initiator).del(id)
          break
  except CatchableError as exc:
    trace "exception occurred", exception = exc.msg
  finally:
    trace "stopping mplex main loop"
    if not m.connection.closed():
      await m.connection.close()

proc newMplex*(conn: Connection,
               maxChanns: uint = MaxChannels): Mplex =
  new result
  result.connection = conn
  result.maxChannels = maxChanns
  result.remote = initTable[uint, LPChannel]()
  result.local = initTable[uint, LPChannel]()

  let m = result
  conn.closeEvent.wait()
  .addCallback do (udata: pointer):
    trace "connection closed, cleaning up mplex"
    asyncCheck m.close()

method newStream*(m: Mplex, name: string = ""): Future[Connection] {.async, gcsafe.} =
  let channel = await m.newStreamInternal()
  # TODO: open the channel (this should be lazy)
  await channel.open()
  result = newConnection(channel)
  result.peerInfo = m.connection.peerInfo

method close*(m: Mplex) {.async, gcsafe.} =
  trace "closing mplex muxer"
  await allFutures(@[allFutures(toSeq(m.remote.values).mapIt(it.reset())),
                      allFutures(toSeq(m.local.values).mapIt(it.reset()))])
