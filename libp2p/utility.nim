## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import 
  stew/byteutils,
  chronos

const
  ShortDumpMax = 12

func shortLog*(item: openarray[byte]): string =
  if item.len <= ShortDumpMax:
    result = item.toHex()
  else:
    const
      split = ShortDumpMax div 2
      dumpLen = (ShortDumpMax * 2) + 3
    result = newStringOfCap(dumpLen)
    result &= item.toOpenArray(0, split - 1).toHex()
    result &= "..."
    result &= item.toOpenArray(item.len - split, item.high).toHex()

func shortLog*(item: string): string =
  if item.len <= ShortDumpMax:
    result = item
  else:
    const
      split = ShortDumpMax div 2
      dumpLen = ShortDumpMax + 3
    result = newStringOfCap(dumpLen)
    result &= item[0..<split]
    result &= "..."
    result &= item[(item.len - split)..item.high]

type
  P2PTracker* = ref object of TrackerBase
    opened*: uint64
    closed*: uint64

  P2PSecondaryTracker* = object
    #For each tracker, save an offset
    trackers: seq[(P2PTracker, uint64)]

proc setupP2PTracker(name: string): P2PTracker =
  let tracker = new P2PTracker

  proc dumpTracking(): string {.gcsafe.} =
    return "Opened " & tracker.id & ": " & $tracker.opened & "\n" &
            "Closed " & tracker.id & ": " & $tracker.closed

  proc leakTransport(): bool {.gcsafe.} =
    return (tracker.opened != tracker.closed)

  tracker.id = name
  tracker.opened = 0
  tracker.closed = 0
  tracker.dump = dumpTracking
  tracker.isLeaked = leakTransport
  addTracker(name, tracker)

  return tracker

proc getP2PTracker*(name: string): P2PTracker {.gcsafe.} =
  result = cast[P2PTracker](getTracker(name))
  if isNil(result):
    result = setupP2PTracker(name)

proc getSecondaryTracker*(trackers: openArray[string]): P2PSecondaryTracker =
  var result: P2PSecondaryTracker
  for trac in trackers:
    let
      tracker = getP2PTracker(trac)
      offset = tracker.opened - tracker.closed
    result.trackers.add((tracker, offset))
  return result

proc isLeaked*(secTracker: P2PSecondaryTracker): bool {.gcsafe.} =
  for t in secTracker.trackers:
    if t[0].opened - t[0].closed != t[1]: return true
  return false

proc dump*(secTracker: P2PSecondaryTracker): string {.gcsafe.} =
  var result = ""
  for t in secTracker.trackers:
    if t[0].opened - t[0].closed != t[1]: result &= t[0].dump() & " (offset " & ($t[1]) & ")"
  return result

when defined(libp2p_agents_metrics):
  import strutils
  export split

  import stew/results
  export results

  proc safeToLowerAscii*(s: string): Result[string, cstring] =
    try:
      ok(s.toLowerAscii())
    except CatchableError:
      err("toLowerAscii failed")

  const
    KnownLibP2PAgents* {.strdefine.} = ""
    KnownLibP2PAgentsSeq* = KnownLibP2PAgents.safeToLowerAscii().tryGet().split(",")
