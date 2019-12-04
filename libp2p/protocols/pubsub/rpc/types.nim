## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import sequtils, options
import chronos, chronicles
import ../../../peerinfo,
       ../../../peer,
       ../../../crypto/crypto

type
    SubOpts* = object
      subscribe*: bool
      topic*: string

    Message* = object
      fromPeer*: seq[byte]
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
      topicId*: string
      messageIDs*: seq[string]

    ControlIWant* = object
      messageIDs*: seq[string]

    ControlGraft* = object
      topicID*: string

    ControlPrune* = object
      topicID*: string

    RPCMsg* = object
      subscriptions*: seq[SubOpts]
      messages*: seq[Message]
      control*: ControlMessage
