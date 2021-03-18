## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[tables, sequtils]
import pkg/[chronos, chronicles, metrics]

import ../upgrademngrs/upgrade,
       ../muxers/muxer

export Upgrade

type
  MuxedUpgrade* = ref object of Upgrade
    muxers*: Table[string, MuxerProvider]
    streamHandler*: StreamHandler

proc identify*(u: MuxedUpgrade, muxer: Muxer) {.async, gcsafe.} =
  # new stream for identify
  var stream = await muxer.newStream()
  if stream == nil:
    return

  try:
    await u.identify(stream)
  finally:
    await stream.closeWithEOF()

proc mux*(u: MuxedUpgrade, conn: Connection): Future[Muxer] {.async, gcsafe.} =
  ## mux incoming connection

  trace "Muxing connection", conn
  if u.muxers.len == 0:
    warn "no muxers registered, skipping upgrade flow", conn
    return

  let muxerName = await u.ms.select(conn, toSeq(u.muxers.keys()))
  if muxerName.len == 0 or muxerName == "na":
    debug "no muxer available, early exit", conn
    return

  trace "Found a muxer", conn, muxerName

  # create new muxer for connection
  let muxer = u.muxers[muxerName].newMuxer(conn)

  # install stream handler
  muxer.streamHandler = u.streamHandler

  u.connManager.storeConn(conn)

  # store it in muxed connections if we have a peer for it
  u.connManager.storeMuxer(muxer, muxer.handle()) # store muxer and start read loop

  try:
    await u.identify(muxer)
  except CancelledError:
    raise
  except CatchableError as exc:
    # Identify is non-essential, though if it fails, it might indicate that
    # the connection was closed already - this will be picked up by the read
    # loop
    debug "Could not identify connection", conn, msg = exc.msg

  return muxer

method upgradeOutgoing*(u: MuxedUpgrade, conn: Connection): Future[Connection] {.async, gcsafe.} =
  trace "Upgrading outgoing connection", conn

  let sconn = await u.secure(conn) # secure the connection
  if isNil(sconn):
    raise newException(UpgradeFailedError,
      "unable to secure connection, stopping upgrade")

  if sconn.peerInfo.isNil:
    raise newException(UpgradeFailedError,
      "current version of nim-libp2p requires that secure protocol negotiates peerid")

  let muxer = await u.mux(sconn) # mux it if possible
  if muxer == nil:
    # TODO this might be relaxed in the future
    raise newException(UpgradeFailedError,
      "a muxer is required for outgoing connections")

  if sconn.closed() or isNil(sconn.peerInfo):
    await sconn.close()
    raise newException(UpgradeFailedError,
      "Connection closed or missing peer info, stopping upgrade")

  trace "Upgraded outgoing connection", conn, sconn

  return sconn

method upgradeIncoming*(u: MuxedUpgrade, incomingConn: Connection) {.async, gcsafe.} = # noraises
  trace "Upgrading incoming connection", incomingConn
  let ms = newMultistream()

  # secure incoming connections
  proc securedHandler(conn: Connection,
                      proto: string)
                      {.async, gcsafe, closure.} =
    trace "Starting secure handler", conn
    let secure = u.secureManagers.filterIt(it.codec == proto)[0]

    var cconn = conn
    try:
      var sconn = await secure.secure(cconn, false)
      if isNil(sconn):
        return

      cconn = sconn
      # add the muxer
      for muxer in u.muxers.values:
        ms.addHandler(muxer.codecs, muxer)

      # handle subsequent secure requests
      await ms.handle(cconn)
    except CatchableError as exc:
      debug "Exception in secure handler during incoming upgrade", msg = exc.msg, conn
      if not cconn.isUpgraded:
        # signal failure
        cconn.upgrade(failed = exc)
      if exc of CancelledError:
        raise
    finally:
      if not isNil(cconn):
        await cconn.close()

    trace "Stopped secure handler", conn

  try:
    if (await ms.select(incomingConn)): # just handshake
      # add the secure handlers
      for k in u.secureManagers:
        ms.addHandler(k.codec, securedHandler)

    # handle un-secured connections
    # we handshaked above, set this ms handler as active
    await ms.handle(incomingConn, active = true)
  except CatchableError as exc:
    debug "Exception upgrading incoming", exc = exc.msg
    if not incomingConn.isUpgraded:
      # signal failure
      incomingConn.upgrade(failed = exc)
    if exc of CancelledError:
        raise
  finally:
    if not isNil(incomingConn):
      await incomingConn.close()

proc muxerHandler(u: MuxedUpgrade, muxer: Muxer) {.async, gcsafe.} =
  let
    conn = muxer.connection

  if conn.peerInfo.isNil:
    warn "This version of nim-libp2p requires secure protocol to negotiate peerid"
    await muxer.close()
    return

  # store incoming connection
  u.connManager.storeConn(conn)

  # store muxer and muxed connection
  u.connManager.storeMuxer(muxer)

  try:
    await u.identify(muxer)
  except IdentifyError as exc:
    # Identify is non-essential, though if it fails, it might indicate that
    # the connection was closed already - this will be picked up by the read
    # loop
    debug "Could not identify connection", conn, msg = exc.msg
  except LPStreamClosedError as exc:
    debug "Identify stream closed", conn, msg = exc.msg
  except LPStreamEOFError as exc:
    debug "Identify stream EOF", conn, msg = exc.msg
  except CancelledError as exc:
    await muxer.close()
    raise exc
  except CatchableError as exc:
    await muxer.close()
    trace "Exception in muxer handler", conn, msg = exc.msg

proc init*(
  T: type MuxedUpgrade,
  identity: Identify,
  muxers: Table[string, MuxerProvider],
  secureManagers: openarray[Secure] = [],
  connManager: ConnManager,
  ms: MultistreamSelect): T =

  let upgrader = T(
    identity: identity,
    muxers: muxers,
    secureManagers: @secureManagers,
    connManager: connManager,
    ms: ms)

  upgrader.streamHandler = proc(conn: Connection) {.async, gcsafe.} = # noraises
    trace "Starting stream handler", conn
    try:
      await upgrader.ms.handle(conn) # handle incoming connection
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "exception in stream handler", conn, msg = exc.msg
    finally:
      await conn.closeWithEOF()
    trace "Stream handler done", conn

  for _, val in muxers:
    val.streamHandler = upgrader.streamHandler
    val.muxerHandler = proc(muxer: Muxer): Future[void] =
      upgrader.muxerHandler(muxer)

  return upgrader
