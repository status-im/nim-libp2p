## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import options, sequtils
import ../../../utility
import ../../../peerid

export options

type
    PeerInfoMsg* = object
      peerID*: seq[byte]
      signedPeerRecord*: seq[byte]

    SubOpts* = object
      subscribe*: bool
      topic*: string

    MessageID* = seq[byte]

    Message* = object
      fromPeer*: PeerId
      data*: seq[byte]
      seqno*: seq[byte]
      topicIDs*: seq[string]
      signature*: seq[byte]
      key*: seq[byte]

    ControlMessage* = object
      ihave*: seq[ControlIHave]
      iwant*: seq[ControlIWant]
      graft*: seq[ControlGraft]
      prune*: seq[ControlPrune]

    ControlIHave* = object
      topicID*: string
      messageIDs*: seq[MessageID]

    ControlIWant* = object
      messageIDs*: seq[MessageID]

    ControlGraft* = object
      topicID*: string

    ControlPrune* = object
      topicID*: string
      peers*: seq[PeerInfoMsg]
      backoff*: uint64

    RPCMsg* = object
      subscriptions*: seq[SubOpts]
      messages*: seq[Message]
      control*: Option[ControlMessage]

func withSubs*(
    T: type RPCMsg, topics: openArray[string], subscribe: bool): T =
  T(
    subscriptions: topics.mapIt(SubOpts(subscribe: subscribe, topic: it)))

func shortLog*(s: ControlIHave): auto =
  (
    topicID: s.topicID.shortLog,
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
    fromPeer: msg.fromPeer.shortLog,
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
