## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options
import chronicles
import messages,
       ../../../peerid,
       ../../../utility,
       ../../../protobuf/minprotobuf

proc write*(pb: var ProtoBuffer, field: int, graft: ControlGraft) =
  var ipb = initProtoBuffer()
  ipb.write(1, graft.topicID)
  ipb.finish()
  pb.write(field, ipb)

proc write*(pb: var ProtoBuffer, field: int, prune: ControlPrune) =
  var ipb = initProtoBuffer()
  ipb.write(1, prune.topicID)
  ipb.finish()
  pb.write(field, ipb)

proc write*(pb: var ProtoBuffer, field: int, ihave: ControlIHave) =
  var ipb = initProtoBuffer()
  ipb.write(1, ihave.topicID)
  for mid in ihave.messageIDs:
    ipb.write(2, mid)
  ipb.finish()
  pb.write(field, ipb)

proc write*(pb: var ProtoBuffer, field: int, iwant: ControlIWant) =
  var ipb = initProtoBuffer()
  for mid in iwant.messageIDs:
    ipb.write(1, mid)
  if len(ipb.buffer) > 0:
    ipb.finish()
    pb.write(field, ipb)

proc write*(pb: var ProtoBuffer, field: int, control: ControlMessage) =
  var ipb = initProtoBuffer()
  for ihave in control.ihave:
    ipb.write(1, ihave)
  for iwant in control.iwant:
    ipb.write(2, iwant)
  for graft in control.graft:
    ipb.write(3, graft)
  for prune in control.prune:
    ipb.write(4, prune)
  if len(ipb.buffer) > 0:
    ipb.finish()
    pb.write(field, ipb)

proc write*(pb: var ProtoBuffer, field: int, subs: SubOpts) =
  var ipb = initProtoBuffer()
  ipb.write(1, uint64(subs.subscribe))
  ipb.write(2, subs.topic)
  ipb.finish()
  pb.write(field, ipb)

proc encodeMessage*(msg: Message): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, msg.fromPeer)
  pb.write(2, msg.data)
  pb.write(3, msg.seqno)
  for topic in msg.topicIDs:
    pb.write(4, topic)
  if len(msg.signature) > 0:
    pb.write(5, msg.signature)
  if len(msg.key) > 0:
    pb.write(6, msg.key)
  pb.finish()
  pb.buffer

proc write*(pb: var ProtoBuffer, field: int, msg: Message) =
  pb.write(field, encodeMessage(msg))

proc decodeGraft*(pb: ProtoBuffer): ProtoResult[ControlGraft] {.
     inline.} =
  trace "decodeGraft: decoding message"
  var control = ControlGraft()
  if ? pb.getField(1, control.topicId):
    trace "decodeGraft: read topicId", topic_id = control.topicId
  else:
    trace "decodeGraft: topicId is missing"
  ok(control)

proc decodePrune*(pb: ProtoBuffer): ProtoResult[ControlPrune] {.
     inline.} =
  trace "decodePrune: decoding message"
  var control = ControlPrune()
  if ? pb.getField(1, control.topicId):
    trace "decodePrune: read topicId", topic_id = control.topicId
  else:
    trace "decodePrune: topicId is missing"
  ok(control)

proc decodeIHave*(pb: ProtoBuffer): ProtoResult[ControlIHave] {.
     inline.} =
  trace "decodeIHave: decoding message"
  var control = ControlIHave()
  if ? pb.getField(1, control.topicId):
    trace "decodeIHave: read topicId", topic_id = control.topicId
  else:
    trace "decodeIHave: topicId is missing"
  if ? pb.getRepeatedField(2, control.messageIDs):
    trace "decodeIHave: read messageIDs", message_ids = control.messageIDs
  else:
    trace "decodeIHave: no messageIDs"
  ok(control)

proc decodeIWant*(pb: ProtoBuffer): ProtoResult[ControlIWant] {.inline.} =
  trace "decodeIWant: decoding message"
  var control = ControlIWant()
  if ? pb.getRepeatedField(1, control.messageIDs):
    trace "decodeIWant: read messageIDs", message_ids = control.messageIDs
  else:
    trace "decodeIWant: no messageIDs"
  ok(control)

proc decodeControl*(pb: ProtoBuffer): ProtoResult[Option[ControlMessage]] {.
     inline.} =
  trace "decodeControl: decoding message"
  var buffer: seq[byte]
  if ? pb.getField(3, buffer):
    var control: ControlMessage
    var cpb = initProtoBuffer(buffer)
    var ihavepbs: seq[seq[byte]]
    var iwantpbs: seq[seq[byte]]
    var graftpbs: seq[seq[byte]]
    var prunepbs: seq[seq[byte]]
    if ? cpb.getRepeatedField(1, ihavepbs):
      for item in ihavepbs:
        control.ihave.add(? decodeIHave(initProtoBuffer(item)))
    if ? cpb.getRepeatedField(2, iwantpbs):
      for item in iwantpbs:
        control.iwant.add(? decodeIWant(initProtoBuffer(item)))
    if ? cpb.getRepeatedField(3, graftpbs):
      for item in graftpbs:
        control.graft.add(? decodeGraft(initProtoBuffer(item)))
    if ? cpb.getRepeatedField(4, prunepbs):
      for item in prunepbs:
        control.prune.add(? decodePrune(initProtoBuffer(item)))
    trace "decodeControl: message statistics", graft_count = len(control.graft),
                                               prune_count = len(control.prune),
                                               ihave_count = len(control.ihave),
                                               iwant_count = len(control.iwant)
    ok(some(control))
  else:
    ok(none[ControlMessage]())

proc decodeSubscription*(pb: ProtoBuffer): ProtoResult[SubOpts] {.inline.} =
  trace "decodeSubscription: decoding message"
  var subflag: uint64
  var sub = SubOpts()
  if ? pb.getField(1, subflag):
    sub.subscribe = bool(subflag)
    trace "decodeSubscription: read subscribe", subscribe = subflag
  else:
    trace "decodeSubscription: subscribe is missing"
  if ? pb.getField(2, sub.topic):
    trace "decodeSubscription: read topic", topic = sub.topic
  else:
    trace "decodeSubscription: topic is missing"
  ok(sub)

proc decodeSubscriptions*(pb: ProtoBuffer): ProtoResult[seq[SubOpts]] {.
     inline.} =
  trace "decodeSubscriptions: decoding message"
  var subpbs: seq[seq[byte]]
  var subs: seq[SubOpts]
  let res = ? pb.getRepeatedField(1, subpbs)
  if res:
    trace "decodeSubscriptions: read subscriptions", count = len(subpbs)
    for item in subpbs:
      subs.add(? decodeSubscription(initProtoBuffer(item)))
    if len(subs) == 0:
      trace "decodeSubscription: no subscriptions found"
  ok(subs)

proc decodeMessage*(pb: ProtoBuffer): ProtoResult[Message] {.inline.} =
  trace "decodeMessage: decoding message"
  var msg: Message
  if ? pb.getField(1, msg.fromPeer):
    trace "decodeMessage: read fromPeer", fromPeer = msg.fromPeer.pretty()
  else:
    trace "decodeMessage: fromPeer is missing"
  if ? pb.getField(2, msg.data):
    trace "decodeMessage: read data", data = msg.data.shortLog()
  else:
    trace "decodeMessage: data is missing"
  if ? pb.getField(3, msg.seqno):
    trace "decodeMessage: read seqno", seqno = msg.seqno
  else:
    trace "decodeMessage: seqno is missing"
  if ? pb.getRepeatedField(4, msg.topicIDs):
    trace "decodeMessage: read topics", topic_ids = msg.topicIDs
  else:
    trace "decodeMessage: topics are missing"
  if ? pb.getField(5, msg.signature):
    trace "decodeMessage: read signature", signature = msg.signature.shortLog()
  else:
    trace "decodeMessage: signature is missing"
  if ? pb.getField(6, msg.key):
    trace "decodeMessage: read public key", key = msg.key.shortLog()
  else:
    trace "decodeMessage: public key is missing"
  ok(msg)

proc decodeMessages*(pb: ProtoBuffer): ProtoResult[seq[Message]] {.inline.} =
  trace "decodeMessages: decoding message"
  var msgpbs: seq[seq[byte]]
  var msgs: seq[Message]
  if ? pb.getRepeatedField(2, msgpbs):
    trace "decodeMessages: read messages", count = len(msgpbs)
    for item in msgpbs:
      msgs.add(? decodeMessage(initProtoBuffer(item)))
  else:
    trace "decodeMessages: no messages found"
  ok(msgs)

proc encodeRpcMsg*(msg: RPCMsg): seq[byte] =
  trace "encodeRpcMsg: encoding message", msg = msg.shortLog()
  var pb = initProtoBuffer()
  for item in msg.subscriptions:
    pb.write(1, item)
  for item in msg.messages:
    pb.write(2, item)
  if msg.control.isSome():
    pb.write(3, msg.control.get())
  if len(pb.buffer) > 0:
    pb.finish()
  pb.buffer

proc decodeRpcMsg*(msg: seq[byte]): ProtoResult[RPCMsg] {.inline.} =
  trace "decodeRpcMsg: decoding message", msg = msg.shortLog()
  var pb = initProtoBuffer(msg)
  var rpcMsg: RPCMsg
  rpcMsg.messages = ? pb.decodeMessages()
  rpcMsg.subscriptions = ? pb.decodeSubscriptions()
  rpcMsg.control = ? pb.decodeControl()
  ok(rpcMsg)
