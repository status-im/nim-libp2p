## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[sugar, tables]

import pkg/[chronos,
            chronicles,
            metrics]

import dial,
       peerid,
       peerinfo,
       multistream,
       connmanager,
       stream/connection,
       transports/transport

export dial

logScope:
  topics = "libp2p dialer"

declareCounter(libp2p_total_dial_attempts, "total attempted dials")
declareCounter(libp2p_successful_dials, "dialed successful peers")
declareCounter(libp2p_failed_dials, "failed dials")
declareCounter(libp2p_failed_upgrades_outgoing, "outgoing connections failed upgrades")

type
  Dialer* = ref object of Dial
    peerInfo*: PeerInfo
    ms: MultistreamSelect
    connManager: ConnManager
    dialLock: Table[PeerID, AsyncLock]
    transports: seq[Transport]

proc dialAndUpgrade(
  self: Dialer,
  peerId: PeerID,
  addrs: seq[MultiAddress]):
  Future[Connection] {.async.} =
  debug "Dialing peer", peerId

  # Avoid "cannot be captured as it would violate memory safety" errors in Nim-1.4.x.
  var
    transport: Transport
    address: MultiAddress

  for t in self.transports: # for each transport
    transport = t
    for a in addrs:      # for each address
      address = a
      if t.handles(a):   # check if it can dial it
        trace "Dialing address", address = $a, peerId
        let dialed = try:
            libp2p_total_dial_attempts.inc()
            # await a connection slot when the total
            # connection count is equal to `maxConns`
            await self.connManager.trackOutgoingConn(
              () => transport.dial(address)
            )
          except TooManyConnectionsError as exc:
            trace "Connection limit reached!"
            raise exc
          except CancelledError as exc:
            debug "Dialing canceled", msg = exc.msg, peerId
            raise exc
          except CatchableError as exc:
            debug "Dialing failed", msg = exc.msg, peerId
            libp2p_failed_dials.inc()
            continue # Try the next address

        # make sure to assign the peer to the connection
        dialed.peerInfo = PeerInfo.init(peerId, addrs)

        # also keep track of the connection's bottom unsafe transport direction
        # required by gossipsub scoring
        dialed.transportDir = Direction.Out

        libp2p_successful_dials.inc()

        let conn = try:
            await transport.upgradeOutgoing(dialed)
          except CatchableError as exc:
            # If we failed to establish the connection through one transport,
            # we won't succeeded through another - no use in trying again
            await dialed.close()
            debug "Upgrade failed", msg = exc.msg, peerId
            if exc isnot CancelledError:
              libp2p_failed_upgrades_outgoing.inc()
            raise exc

        doAssert not isNil(conn), "connection died after upgradeOutgoing"
        debug "Dial successful", conn, peerInfo = conn.peerInfo
        return conn

proc internalConnect(
  self: Dialer,
  peerId: PeerID,
  addrs: seq[MultiAddress]):
  Future[Connection] {.async.} =
  if self.peerInfo.peerId == peerId:
    raise newException(CatchableError, "can't dial self!")

  # Ensure there's only one in-flight attempt per peer
  let lock = self.dialLock.mgetOrPut(peerId, newAsyncLock())
  try:
    await lock.acquire()

    # Check if we have a connection already and try to reuse it
    var conn = self.connManager.selectConn(peerId)
    if conn != nil:
      if conn.atEof or conn.closed:
        # This connection should already have been removed from the connection
        # manager - it's essentially a bug that we end up here - we'll fail
        # for now, hoping that this will clean themselves up later...
        warn "dead connection in connection manager", conn
        await conn.close()
        raise newException(DialFailedError, "Zombie connection encountered")

      trace "Reusing existing connection", conn, direction = $conn.dir
      return conn

    conn = await self.dialAndUpgrade(peerId, addrs)
    if isNil(conn): # None of the addresses connected
      raise newException(DialFailedError, "Unable to establish outgoing link")

    # We already check for this in Connection manager
    # but a disconnect could have happened right after
    # we've added the connection so we check again
    # to prevent races due to that.
    if conn.closed() or conn.atEof():
      # This can happen when the other ends drops us
      # before we get a chance to return the connection
      # back to the dialer.
      trace "Connection dead on arrival", conn
      raise newLPStreamClosedError()

    return conn
  finally:
    if lock.locked():
      lock.release()

method connect*(
  self: Dialer,
  peerId: PeerID,
  addrs: seq[MultiAddress])
  {.async, raises: [Defect, DialFailedError].} =
  ## connect remote peer without negotiating
  ## a protocol
  ##

  if self.connManager.connCount(peerId) > 0:
    return

  discard await self.internalConnect(peerId, addrs)

proc negotiateStream(
  self: Dialer,
  conn: Connection,
  protos: seq[string]): Future[Connection] {.async.} =
  trace "Negotiating stream", conn, protos
  let selected = await self.ms.select(conn, protos)
  if not protos.contains(selected):
    await conn.closeWithEOF()
    raise newException(DialFailedError, "Unable to select sub-protocol " & $protos)

  return conn

method dial*(
  self: Dialer,
  peerId: PeerID,
  protos: seq[string]): Future[Connection]
  {.async, raises: [Defect, DialFailedError].} =
  ## create a protocol stream over an
  ## existing connection
  ##

  trace "Dialing (existing)", peerId, protos
  let stream = await self.connManager.getStream(peerId)
  if stream.isNil:
    raise newException(DialFailedError, "Couldn't get muxed stream")

  return await self.negotiateStream(stream, protos)

method dial*(
  self: Dialer,
  peerId: PeerID,
  addrs: seq[MultiAddress],
  protos: seq[string]): Future[Connection]
  {.async, raises: [Defect, DialFailedError].} =
  ## create a protocol stream and establish
  ## a connection if one doesn't exist already
  ##

  var
    conn: Connection
    stream: Connection

  proc cleanup() {.async.} =
    if not(isNil(stream)):
      await stream.closeWithEOF()

    if not(isNil(conn)):
      await conn.close()

  try:
    trace "Dialing (new)", peerId, protos
    conn = await self.internalConnect(peerId, addrs)
    trace "Opening stream", conn
    stream = await self.connManager.getStream(conn)

    if isNil(stream):
      raise newException(DialFailedError,
        "Couldn't get muxed stream")

    return await self.negotiateStream(stream, protos)
  except CatchableError as exc:
    debug "Exception dialing", conn, msg = exc.msg
    await cleanup()
    raise exc

proc new*(
  T: type Dialer,
  peerInfo: PeerInfo,
  connManager: ConnManager,
  transports: seq[Transport],
  ms: MultistreamSelect): Dialer =

  T(peerInfo: peerInfo,
    connManager: connManager,
    transports: transports,
    ms: ms)
