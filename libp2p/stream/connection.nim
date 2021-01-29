## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[hashes, oids, strformat]
import chronicles, chronos, metrics
import lpstream,
       ../multiaddress,
       ../peerinfo,
       ../errors

export lpstream, peerinfo, errors

logScope:
  topics = "libp2p connection"

const
  ConnectionTrackerName* = "Connection"
  DefaultConnectionTimeout* = 5.minutes

type
  TimeoutHandler* = proc(): Future[void] {.gcsafe.}

  Connection* = ref object of LPStream
    activity*: bool                 # reset every time data is sent or received
    timeout*: Duration              # channel timeout if no activity
    timerTaskFut: Future[void]      # the current timer instance
    timeoutHandler*: TimeoutHandler # timeout handler
    peerInfo*: PeerInfo
    observedAddr*: Multiaddress
    upgraded*: Future[void]

proc timeoutMonitor(s: Connection) {.async, gcsafe.}

proc isUpgraded*(s: Connection): bool =
  if not isNil(s.upgraded):
    return s.upgraded.finished

proc upgrade*(s: Connection, failed: ref Exception = nil) =
  if not isNil(s.upgraded):
    if not isNil(failed):
      s.upgraded.fail(failed)
      return

    s.upgraded.complete()

proc onUpgrade*(s: Connection) {.async.} =
  if not isNil(s.upgraded):
    await s.upgraded

func shortLog*(conn: Connection): string =
  if conn.isNil: "Connection(nil)"
  elif conn.peerInfo.isNil: $conn.oid
  else: &"{shortLog(conn.peerInfo.peerId)}:{conn.oid}"
chronicles.formatIt(Connection): shortLog(it)

method initStream*(s: Connection) =
  if s.objName.len == 0:
    s.objName = "Connection"

  procCall LPStream(s).initStream()

  doAssert(isNil(s.timerTaskFut))

  if isNil(s.upgraded):
    s.upgraded = newFuture[void]()

  if s.timeout > 0.millis:
    trace "Monitoring for timeout", s, timeout = s.timeout

    s.timerTaskFut = s.timeoutMonitor()
    if isNil(s.timeoutHandler):
      s.timeoutHandler = proc(): Future[void] =
        trace "Idle timeout expired, closing connection", s
        s.close()

method closeImpl*(s: Connection): Future[void] =
  # Cleanup timeout timer
  trace "Closing connection", s

  if not isNil(s.timerTaskFut) and not s.timerTaskFut.finished:
    s.timerTaskFut.cancel()
    s.timerTaskFut = nil

  if not isNil(s.upgraded) and not s.upgraded.finished:
    s.upgraded.cancel()
    s.upgraded = nil

  trace "Closed connection", s

  procCall LPStream(s).closeImpl()

func hash*(p: Connection): Hash =
  cast[pointer](p).hash

proc pollActivity(s: Connection): Future[bool] {.async.} =
  if s.closed and s.atEof:
    return false # Done, no more monitoring

  if s.activity:
    s.activity = false
    return true

  # Inactivity timeout happened, call timeout monitor

  trace "Connection timed out", s
  if not(isNil(s.timeoutHandler)):
    trace "Calling timeout handler", s

    try:
      await s.timeoutHandler()
    except CancelledError:
      # timeoutHandler is expected to be fast, but it's still possible that
      # cancellation will happen here - no need to warn about it - we do want to
      # stop the polling however
      debug "Timeout handler cancelled", s
    except CatchableError as exc: # Shouldn't happen
      warn "exception in timeout handler", s, exc = exc.msg

  return false

proc timeoutMonitor(s: Connection) {.async, gcsafe.} =
  ## monitor the channel for inactivity
  ##
  ## if the timeout was hit, it means that
  ## neither incoming nor outgoing activity
  ## has been detected and the channel will
  ## be reset
  ##

  while true:
    try: # Sleep at least once!
      await sleepAsync(s.timeout)
    except CancelledError:
      return

    if not await s.pollActivity():
      return

proc init*(C: type Connection,
           peerInfo: PeerInfo,
           dir: Direction,
           timeout: Duration = DefaultConnectionTimeout,
           timeoutHandler: TimeoutHandler = nil,
           observedAddr: MultiAddress = MultiAddress()): Connection =
  result = C(peerInfo: peerInfo,
             dir: dir,
             timeout: timeout,
             timeoutHandler: timeoutHandler,
             observedAddr: observedAddr)

  result.initStream()
