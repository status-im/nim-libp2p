## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos, chronicles
import tables, options, sets, sequtils
import rpc/[messages, message], ../utils/timedcache

type
  CacheEntry* = object
    mid*: string
    msg*: Message

  MCache* = ref object of RootObj
    msgs*: TimedCache[Message]
    history*: seq[seq[CacheEntry]]
    historySize*: Natural
    windowSize*: Natural

proc put*(c: MCache, msg: Message) =
  proc handler(key: string, val: Message) {.gcsafe.} =
    ## make sure we remove the message from history
    ## to keep things consisten
    c.history.applyIt(
      it.filterIt(it.mid != msg.msgId)
    )

  c.msgs.put(msg.msgId, msg, handler = handler)
  c.history[0].add(CacheEntry(mid: msg.msgId, msg: msg))

proc get*(c: MCache, mid: string): Option[Message] =
  result = none(Message)
  if mid in c.msgs:
    result = some(c.msgs[mid])

proc window*(c: MCache, topic: string): HashSet[string] =
  result = initHashSet[string]()

  let len =
    if c.windowSize > c.history.len:
      c.history.len
    else:
      c.windowSize

  if c.history.len > 0:
    for slot in c.history[0..<len]:
      for entry in slot:
        for t in entry.msg.topicIDs:
          if t == topic:
            result.incl(entry.msg.msgId)
            break

proc shift*(c: MCache) =
  while c.history.len > c.historySize:
    for entry in c.history.pop():
      c.msgs.del(entry.mid)

  c.history.insert(@[])

proc newMCache*(window: Natural, history: Natural): MCache =
  new result
  result.historySize = history
  result.windowSize = window
  result.history = newSeq[seq[CacheEntry]]()
  result.history.add(@[]) # initialize with empty slot
  result.msgs = newTimedCache[Message](2.minutes)
