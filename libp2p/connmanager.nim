## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[options, tables, sequtils, sets]
import chronos, chronicles, metrics
import peerinfo,
       stream/connection,
       muxers/muxer,
       utils/semaphore,
       errors

logScope:
  topics = "libp2p connmanager"

declareGauge(libp2p_peers, "total connected peers")

const
  MaxConnections* = 50
  MaxConnectionsPerPeer* = 5

type
  TooManyConnectionsError* = object of LPError

  ConnProvider* = proc(): Future[Connection]
    {.gcsafe, closure, raises: [Defect].}

  ConnEventKind* {.pure.} = enum
    Connected,    # A connection was made and securely upgraded - there may be
                  # more than one concurrent connection thus more than one upgrade
                  # event per peer.

    Disconnected  # Peer disconnected - this event is fired once per upgrade
                  # when the associated connection is terminated.

  ConnEvent* = object
    case kind*: ConnEventKind
    of ConnEventKind.Connected:
      incoming*: bool
    else:
      discard

  ConnEventHandler* =
    proc(peerId: PeerID, event: ConnEvent): Future[void]
      {.gcsafe, raises: [Defect].}

  PeerEventKind* {.pure.} = enum
    Left,
    Joined

  PeerEvent* = object
    case kind*: PeerEventKind
      of PeerEventKind.Joined:
        initiator*: bool
      else:
        discard

  PeerEventHandler* =
    proc(peerId: PeerID, event: PeerEvent): Future[void] {.gcsafe.}

  MuxerHolder = object
    muxer: Muxer
    handle: Future[void]

  ConnManager* = ref object of RootObj
    maxConnsPerPeer: int
    inSema*: AsyncSemaphore
    outSema*: AsyncSemaphore
    conns: Table[PeerID, HashSet[Connection]]
    muxed: Table[Connection, MuxerHolder]
    connEvents: Table[ConnEventKind, OrderedSet[ConnEventHandler]]
    peerEvents: Table[PeerEventKind, OrderedSet[PeerEventHandler]]

proc newTooManyConnectionsError(): ref TooManyConnectionsError {.inline.} =
  result = newException(TooManyConnectionsError, "Too many connections")

proc init*(C: type ConnManager,
           maxConnsPerPeer = MaxConnectionsPerPeer,
           maxConnections = MaxConnections,
           maxIn = -1,
           maxOut = -1): ConnManager =
  var inSema, outSema: AsyncSemaphore
  if maxIn > 0 or maxOut > 0:
    inSema = newAsyncSemaphore(maxIn)
    outSema = newAsyncSemaphore(maxOut)
  elif maxConnections > 0:
    inSema = newAsyncSemaphore(maxConnections)
    outSema = inSema
  else:
    raiseAssert "Invalid connection counts!"

  C(maxConnsPerPeer: maxConnsPerPeer,
    inSema: inSema,
    outSema: outSema)

proc connCount*(c: ConnManager, peerId: PeerID): int =
  c.conns.getOrDefault(peerId).len

proc addConnEventHandler*(c: ConnManager,
                          handler: ConnEventHandler,
                          kind: ConnEventKind) =
  ## Add peer event handler - handlers must not raise exceptions!
  ##

  try:
    if isNil(handler): return
    c.connEvents.mgetOrPut(kind,
      initOrderedSet[ConnEventHandler]()).incl(handler)
  except Exception as exc:
    # TODO: there is an Exception being raised
    # somewhere in the depths of the std.
    # Not sure what to do with it here, it seems
    # like we should just quit right away because
    # there is no way of telling what happened

    raiseAssert exc.msg

proc removeConnEventHandler*(c: ConnManager,
                             handler: ConnEventHandler,
                             kind: ConnEventKind) =

  try:
    c.connEvents.withValue(kind, handlers) do:
      handlers[].excl(handler)
  except Exception as exc:
    # TODO: there is an Exception being raised
    # somewhere in the depths of the std.
    # Not sure what to do with it here, it seems
    # like we should just quit right away because
    # there is no way of telling what happened

    raiseAssert exc.msg

proc triggerConnEvent*(c: ConnManager,
                       peerId: PeerID,
                       event: ConnEvent) {.async, gcsafe.} =
  try:
    trace "About to trigger connection events", peer = peerId
    if event.kind in c.connEvents:
      trace "triggering connection events", peer = peerId, event = $event.kind
      var connEvents: seq[Future[void]]
      for h in c.connEvents[event.kind]:
        connEvents.add(h(peerId, event))

      checkFutures(await allFinished(connEvents))
  except LPError as exc:
    warn "Exception in triggerConnEvents",
      msg = exc.msg, peerId, event = $event

proc addPeerEventHandler*(c: ConnManager,
                          handler: PeerEventHandler,
                          kind: PeerEventKind) =
  ## Add peer event handler - handlers must not raise exceptions!
  ##

  try:
    if isNil(handler): return
    c.peerEvents.mgetOrPut(kind,
      initOrderedSet[PeerEventHandler]()).incl(handler)
  except Exception as exc:
    # TODO: there is an Exception being raised
    # somewhere in the depths of the std.
    # Not sure what to do with it here, it seems
    # like we should just quit right away because
    # there is no way of telling what happened

    raiseAssert exc.msg

proc removePeerEventHandler*(c: ConnManager,
                             handler: PeerEventHandler,
                             kind: PeerEventKind) =
  try:
    c.peerEvents.withValue(kind, handlers) do:
      handlers[].excl(handler)
  except Exception as exc:
    # TODO: there is an Exception being raised
    # somewhere in the depths of the std.
    # Not sure what to do with it here, it seems
    # like we should just quit right away because
    # there is no way of telling what happened

    raiseAssert exc.msg

proc triggerPeerEvents*(c: ConnManager,
                        peerId: PeerID,
                        event: PeerEvent) {.async, gcsafe.} =

  trace "About to trigger peer events", peer = peerId
  if event.kind notin c.peerEvents:
    return

  try:
    let count = c.connCount(peerId)
    if event.kind == PeerEventKind.Joined and count != 1:
      trace "peer already joined", peerId, event = $event
      return
    elif event.kind == PeerEventKind.Left and count != 0:
      trace "peer still connected or already left", peerId, event = $event
      return

    trace "triggering peer events", peerId, event = $event

    var peerEvents: seq[Future[void]]
    try:
      for h in c.peerEvents[event.kind]:
        peerEvents.add(h(peerId, event))
    except Exception as exc:
      raiseAssert exc.msg

    checkFutures(await allFinished(peerEvents))
  except LPError as exc: # handlers should not raise!
    warn "Exception in triggerPeerEvents", exc = exc.msg, peerId

proc contains*(c: ConnManager, conn: Connection): bool =
  ## checks if a connection is being tracked by the
  ## connection manager
  ##

  if isNil(conn):
    return

  if isNil(conn.peerInfo):
    return

  return conn in c.conns.getOrDefault(conn.peerInfo.peerId)

proc contains*(c: ConnManager, peerId: PeerID): bool =
  peerId in c.conns

proc contains*(c: ConnManager, muxer: Muxer): bool =
  ## checks if a muxer is being tracked by the connection
  ## manager
  ##

  if isNil(muxer):
    return

  let conn = muxer.connection
  if conn notin c:
    return

  if conn notin c.muxed:
    return

  return muxer == c.muxed.getOrDefault(conn).muxer

proc closeMuxerHolder(muxerHolder: MuxerHolder) {.async.} =
  trace "Cleaning up muxer", m = muxerHolder.muxer

  await muxerHolder.muxer.close()
  if not(isNil(muxerHolder.handle)):
    try:
      await muxerHolder.handle # TODO noraises?
    except CatchableError as exc:
      trace "Exception in close muxer handler", exc = exc.msg
  trace "Cleaned up muxer", m = muxerHolder.muxer

proc delConn(c: ConnManager, conn: Connection) =
  let peerId = conn.peerInfo.peerId
  if peerId in c.conns:
    c.conns.withValue(peerId, conns):
      conns[].excl(conn)

    if c.conns.getOrDefault(peerId).len <= 0:
      c.conns.del(peerId)

    libp2p_peers.set(c.conns.len.int64)
    trace "Removed connection", conn

proc cleanupConn(c: ConnManager, conn: Connection) {.async.} =
  ## clean connection's resources such as muxers and streams

  if isNil(conn):
    trace "Wont cleanup a nil connection"
    return

  if isNil(conn.peerInfo):
    trace "No peer info for connection"
    return

  # Remove connection from all tables without async breaks
  var muxer = some(MuxerHolder())
  if not c.muxed.pop(conn, muxer.get()):
    muxer = none(MuxerHolder)

  delConn(c, conn)

  try:
    if muxer.isSome:
      await closeMuxerHolder(muxer.get())
  finally:
    await conn.close()

  trace "Connection cleaned up", conn

proc onConnUpgraded(c: ConnManager, conn: Connection) {.async.} =
  try:
    trace "Triggering connect events", conn
    conn.upgrade()

    let peerId = conn.peerInfo.peerId
    await c.triggerPeerEvents(
      peerId, PeerEvent(kind: PeerEventKind.Joined, initiator: conn.dir == Direction.Out))

    await c.triggerConnEvent(
      peerId, ConnEvent(kind: ConnEventKind.Connected, incoming: conn.dir == Direction.In))
  except CatchableError as exc:
    warn "Unexpected exception in switch peer connection cleanup",
      conn, msg = exc.msg

proc peerCleanup(c: ConnManager, conn: Connection) {.async.} =
  try:
    trace "Triggering disconnect events", conn
    let peerId = conn.peerInfo.peerId
    await c.triggerConnEvent(
      peerId, ConnEvent(kind: ConnEventKind.Disconnected))
    await c.triggerPeerEvents(peerId, PeerEvent(kind: PeerEventKind.Left))
  except CatchableError as exc:
    warn "Unexpected exception peer cleanup handler",
      conn, msg = exc.msg

proc onClose(c: ConnManager, conn: Connection) {.async.} =
  ## connection close even handler
  ##
  ## triggers the connections resource cleanup
  ##
  try:
    await conn.join()
    trace "Connection closed, cleaning up", conn
    await c.cleanupConn(conn)
  except CatchableError as exc:
    debug "Exception in connection manager's cleanup",
          errMsg = exc.msg, conn
  finally:
    trace "Triggering peerCleanup", conn
    asyncSpawn c.peerCleanup(conn)

proc selectConn*(c: ConnManager,
                peerId: PeerID,
                dir: Direction): Connection =
  ## Select a connection for the provided peer and direction
  ##
  let conns = toSeq(
    c.conns.getOrDefault(peerId))
    .filterIt( it.dir == dir )

  if conns.len > 0:
    return conns[0]

proc selectConn*(c: ConnManager, peerId: PeerID): Connection =
  ## Select a connection for the provided giving priority
  ## to outgoing connections
  ##

  var conn = c.selectConn(peerId, Direction.Out)
  if isNil(conn):
    conn = c.selectConn(peerId, Direction.In)
  if isNil(conn):
    trace "connection not found", peerId

  return conn

proc selectMuxer*(c: ConnManager, conn: Connection): Muxer =
  ## select the muxer for the provided connection
  ##

  if isNil(conn):
    return

  if conn in c.muxed:
    return c.muxed.getOrDefault(conn).muxer
  else:
    debug "no muxer for connection", conn

proc storeConn*(c: ConnManager, conn: Connection)
  {.raises: [Defect, LPError].} =
  ## store a connection
  ##

  if isNil(conn):
    raise newException(LPError, "Connection cannot be nil")

  if conn.closed or conn.atEof:
    raise newException(LPStreamEOFError, "Connection closed or EOF")

  if isNil(conn.peerInfo):
    raise newException(LPError, "Empty peer info")

  let peerId = conn.peerInfo.peerId
  if c.conns.getOrDefault(peerId).len > c.maxConnsPerPeer:
    debug "Too many connections for peer",
      conn, conns = c.conns.getOrDefault(peerId).len

    raise newTooManyConnectionsError()

  if peerId notin c.conns:
    c.conns[peerId] = initHashSet[Connection]()

  c.conns.mgetOrPut(peerId,
    initHashSet[Connection]()).incl(conn)

  # c.conns.getOrDefault(peerId).incl(conn)
  libp2p_peers.set(c.conns.len.int64)

  # Launch on close listener
  # All the errors are handled inside `onClose()` procedure.
  asyncSpawn c.onClose(conn)

  trace "Stored connection",
    conn, direction = $conn.dir, connections = c.conns.len

proc trackConn(c: ConnManager,
               provider: ConnProvider,
               sema: AsyncSemaphore):
               Future[Connection] {.async.} =
  var conn: Connection
  try:
    conn = await provider()

    if isNil(conn):
      return

    trace "Got connection", conn

    proc semaphoreMonitor() {.async.} =
      try:
        await conn.join()
      except CatchableError as exc:
        trace "Exception in semaphore monitor, ignoring", exc = exc.msg

      sema.release()

    asyncSpawn semaphoreMonitor()
  except CatchableError as exc:
    trace "Exception tracking connection", exc = exc.msg
    if not isNil(conn):
      await conn.close()

    raise exc

  return conn

proc trackIncomingConn*(c: ConnManager,
                        provider: ConnProvider):
                        Future[Connection] {.async.} =
  ## await for a connection slot before attempting
  ## to call the connection provider
  ##

  var conn: Connection
  try:
    trace "Tracking incoming connection"
    await c.inSema.acquire()
    conn = await c.trackConn(provider, c.inSema)
    if isNil(conn):
      trace "Couldn't acquire connection, releasing semaphore slot", dir = $Direction.In
      c.inSema.release()

    return conn
  except CatchableError as exc:
    trace "Exception tracking connection", exc = exc.msg
    c.inSema.release()
    raise exc

proc trackOutgoingConn*(c: ConnManager,
                        provider: ConnProvider):
                        Future[Connection] {.async.} =
  ## try acquiring a connection if all slots
  ## are already taken, raise TooManyConnectionsError
  ## exception
  ##

  trace "Tracking outgoing connection", count = c.outSema.count,
                                        max = c.outSema.size

  if not c.outSema.tryAcquire():
    trace "Too many outgoing connections!", count = c.outSema.count,
                                            max = c.outSema.size
    raise newTooManyConnectionsError()

  var conn: Connection
  try:
    conn = await c.trackConn(provider, c.outSema)
    if isNil(conn):
      trace "Couldn't acquire connection, releasing semaphore slot", dir = $Direction.Out
      c.outSema.release()

    return conn
  except CatchableError as exc:
    trace "Exception tracking connection", exc = exc.msg
    c.outSema.release()
    raise exc

proc storeMuxer*(c: ConnManager,
                 muxer: Muxer,
                 handle: Future[void] = nil) {.raises: [Defect, LPError].} =
  ## store the connection and muxer
  ##

  if isNil(muxer):
    raise newException(LPError, "muxer cannot be nil")

  if isNil(muxer.connection):
    raise newException(LPError, "muxer's connection cannot be nil")

  if muxer.connection notin c:
    raise newException(LPError, "cant add muxer for untracked connection")

  c.muxed[muxer.connection] = MuxerHolder(
    muxer: muxer,
    handle: handle)

  trace "Stored muxer",
    muxer, handle = not handle.isNil, connections = c.conns.len

  asyncSpawn c.onConnUpgraded(muxer.connection)

proc getStream*(c: ConnManager,
                peerId: PeerID,
                dir: Direction): Future[Connection] {.async, gcsafe.} =
  ## get a muxed stream for the provided peer
  ## with the given direction
  ##

  let muxer = c.selectMuxer(c.selectConn(peerId, dir))
  if not(isNil(muxer)):
    return await muxer.newStream()

proc getStream*(c: ConnManager,
                peerId: PeerID): Future[Connection] {.async, gcsafe.} =
  ## get a muxed stream for the passed peer from any connection
  ##

  let muxer = c.selectMuxer(c.selectConn(peerId))
  if not(isNil(muxer)):
    return await muxer.newStream()

proc getStream*(c: ConnManager,
                conn: Connection): Future[Connection] {.async, gcsafe.} =
  ## get a muxed stream for the passed connection
  ##

  let muxer = c.selectMuxer(conn)
  if not(isNil(muxer)):
    return await muxer.newStream()

proc dropPeer*(c: ConnManager, peerId: PeerID) {.async.} =
  ## drop connections and cleanup resources for peer
  ##
  trace "Dropping peer", peerId
  let conns = c.conns.getOrDefault(peerId)
  for conn in conns:
    trace  "Removing connection", conn
    delConn(c, conn)

  var muxers: seq[MuxerHolder]
  for conn in conns:
    if conn in c.muxed:
      muxers.add c.muxed[conn]
      c.muxed.del(conn)

  for muxer in muxers:
    await closeMuxerHolder(muxer)

  for conn in conns:
    await conn.close()
    trace "Dropped peer", peerId

  trace "Peer dropped", peerId

proc close*(c: ConnManager) {.async.} =
  ## cleanup resources for the connection
  ## manager
  ##

  trace "Closing ConnManager"
  let conns = c.conns
  c.conns.clear()

  let muxed = c.muxed
  c.muxed.clear()

  for _, muxer in muxed:
    await closeMuxerHolder(muxer)

  for _, conns2 in conns:
    for conn in conns2:
      await conn.close()

  trace "Closed ConnManager"
