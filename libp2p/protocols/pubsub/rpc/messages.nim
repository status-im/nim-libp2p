## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import options, sequtils
import protobuf_serialization
import ../../../utility
import ../../../peerid

export options
export protobuf_serialization

type
    SubOpts* {.protobuf2.} = object
      subscribe* {.fieldNumber: 1, required.}: bool
      topic* {.fieldNumber: 2, required.}: string

    Message* {.protobuf2.} = object
      fromPeer* {.fieldNumber: 1, required.}: seq[byte]
      data* {.fieldNumber: 2, required.}: seq[byte]
      seqno* {.fieldNumber: 3, required.}: seq[byte]
      topicIDs* {.fieldNumber: 4.}: seq[string]
      signature* {.fieldNumber: 5.}: seq[byte]
      key* {.fieldNumber: 6.}: seq[byte]

    ControlMessage* {.protobuf2.} = object
      ihave* {.fieldNumber: 1.}: seq[ControlIHave]
      iwant* {.fieldNumber: 2.}: seq[ControlIWant]
      graft* {.fieldNumber: 3.}: seq[ControlGraft]
      prune* {.fieldNumber: 4.}: seq[ControlPrune]

    ControlIHave* {.protobuf2.} = object
      topicID* {.fieldNumber: 1.}: PBOption[""]
      messageIDs* {.fieldNumber: 2.}: seq[string]

    ControlIWant* {.protobuf2.} = object
      messageIDs* {.fieldNumber: 1.}: seq[string]

    ControlGraft* {.protobuf2.} = object
      topicID* {.fieldNumber: 1, required.}: string

    ControlPrune* {.protobuf2.} = object
      topicID* {.fieldNumber: 1, required.}: string

    RPCMsg* {.protobuf2.} = object
      subscriptions* {.fieldNumber: 1.}: seq[SubOpts]
      messages* {.fieldNumber: 2.}: seq[Message]
      control* {.fieldNumber: 3.}: Option[ControlMessage]

func shortLog*(s: ControlIHave): auto =
  (
    topicID: s.topicID.get().shortLog,
    messageIDs: mapIt(s.messageIDs, it.shortLog)
  )

func shortLog*(s: ControlIWant): auto =
  (
    messageIDs: mapIt(s.messageIDs, it.shortLog)
  )

func shortLog*(s: ControlGraft): auto =
  (
    topicID: s.topicID.shortLog
  )

func shortLog*(s: ControlPrune): auto =
  (
    topicID: s.topicID.shortLog
  )

func shortLog*(c: ControlMessage): auto =
  (
    ihave: mapIt(c.ihave, it.shortLog),
    iwant: mapIt(c.iwant, it.shortLog),
    graft: mapIt(c.graft, it.shortLog),
    prune: mapIt(c.prune, it.shortLog)
  )

func shortLog*(msg: Message): auto =
  (
    fromPeer: msg.fromPeer,
    data: msg.data.shortLog,
    seqno: msg.seqno.shortLog,
    topicIDs: $msg.topicIDs,
    signature: msg.signature.shortLog,
    key: msg.key.shortLog
  )

func shortLog*(m: RPCMsg): auto =
  if m.control.isSome:
    (
      subscriptions: m.subscriptions,
      messages: mapIt(m.messages, it.shortLog),
      control: m.control.get().shortLog
    )
  else:
    (
      subscriptions: m.subscriptions,
      messages: mapIt(m.messages, it.shortLog),
      control: ControlMessage().shortLog
    )
