## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronicles
import messages, 
       ../../../protobuf/minprotobuf

proc encodeMessage*(msg: Message, buff: var ProtoBuffer) {.gcsafe.} = 
  buff.write(initProtoField(1, msg.fromPeer))
  buff.write(initProtoField(2, msg.data))
  buff.write(initProtoField(3, msg.seqno))

  for t in msg.topicIDs:
    buff.write(initProtoField(4, t))

  if msg.signature.len > 0:
    buff.write(initProtoField(5, msg.signature))
  
  if msg.key.len > 0:
    buff.write(initProtoField(6, msg.key))

  buff.finish()

proc encodeSubs*(subs: SubOpts, buff: var ProtoBuffer) {.gcsafe.} = 
  buff.write(initProtoField(1, subs.subscribe))
  buff.write(initProtoField(2, subs.topic))

proc encodeRpcMsg*(msg: RPCMsg): ProtoBuffer {.gcsafe.} = 
  result = initProtoBuffer()
  trace "encoding msg: ", msg = msg

  if msg.subscriptions.len > 0:
    var subs = initProtoBuffer()
    for s in msg.subscriptions:
      encodeSubs(s, subs)

    # write subscriptions to protobuf
    subs.finish()
    result.write(initProtoField(1, subs))

  if msg.messages.len > 0:
    var messages = initProtoBuffer()
    for m in msg.messages:
      encodeMessage(m, messages)

    # write messages to protobuf
    messages.finish()
    result.write(initProtoField(2, messages))

  if result.buffer.len > 0:
    result.finish()

proc decodeRpcMsg*(msg: seq[byte]): RPCMsg {.gcsafe.} = 
  var pb = initProtoBuffer(msg)

  result.subscriptions = newSeq[SubOpts]()
  while true:
    # decode SubOpts array
    var field = pb.enterSubMessage()
    trace "processing submessage", field = field
    case field:
    of 0:
      break
    of 1:
      while true:
        var subOpt: SubOpts
        var subscr: int
        discard pb.getVarintValue(1, subscr)
        subOpt.subscribe = cast[bool](subscr)
        trace "read subscribe field", subscribe = subOpt.subscribe

        if pb.getString(2, subOpt.topic) < 0:
          break
        trace "read subscribe field", topicName = subOpt.topic

        result.subscriptions.add(subOpt)
      trace "got subscriptions", subscriptions = result.subscriptions

    of 2:
      result.messages = newSeq[Message]()
      # TODO: which of this fields are really optional?
      while true:
        var msg: Message
        if pb.getBytes(1, msg.fromPeer) < 0:
          break
        trace "read message field", fromPeer = msg.fromPeer

        if pb.getBytes(2, msg.data) < 0:
          break
        trace "read message field", data = msg.data

        if pb.getBytes(3, msg.seqno) < 0:
          break
        trace "read message field", seqno = msg.seqno

        var topic: string
        while true:
          if pb.getString(4, topic) < 0:
            break
          msg.topicIDs.add(topic)
          trace "read message field", topicName = topic
          topic = ""
        
        discard pb.getBytes(5, msg.signature)
        trace "read message field", signature = msg.signature

        discard pb.getBytes(6, msg.key)
        trace "read message field", key = msg.key

        result.messages.add(msg)
    else: 
      raise newException(CatchableError, "message type not recognized")
