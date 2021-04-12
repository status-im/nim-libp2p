## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import std/[options, strformat]
import chronos, chronicles, bearssl
import ../protocol,
       ../../stream/streamseq,
       ../../stream/connection,
       ../../multiaddress,
       ../../peerinfo,
       ../../errors

{.push raises: [Defect].}

export protocol

logScope:
  topics = "libp2p secure"

const
  SecureConnTrackerName* = "SecureConn"

type
  Secure* = ref object of LPProtocol # base type for secure managers

  SecureConn* = ref object of Connection
    stream*: Connection
    buf: StreamSeq

func shortLog*(conn: SecureConn): auto =
  try:
    if conn.isNil: "SecureConn(nil)"
    elif conn.peerInfo.isNil: $conn.oid
    else: &"{shortLog(conn.peerInfo.peerId)}:{conn.oid}"
  except ValueError as exc:
    raiseAssert(exc.msg)

chronicles.formatIt(SecureConn): shortLog(it)

proc init*(T: type SecureConn,
           conn: Connection,
           peerInfo: PeerInfo,
           observedAddr: Multiaddress,
           timeout: Duration = DefaultConnectionTimeout): T =
  result = T(stream: conn,
             peerInfo: peerInfo,
             observedAddr: observedAddr,
             closeEvent: conn.closeEvent,
             upgraded: conn.upgraded,
             timeout: timeout,
             dir: conn.dir)
  result.initStream()

method initStream*(s: SecureConn) =
  if s.objName.len == 0:
    s.objName = "SecureConn"

  procCall Connection(s).initStream()

method closeImpl*(s: SecureConn) {.async.} =
  trace "Closing secure conn", s, dir = s.dir
  if not(isNil(s.stream)):
    await s.stream.close()

  await procCall Connection(s).closeImpl()

method readMessage*(c: SecureConn): Future[seq[byte]] {.async, base.} =
  doAssert(false, "Not implemented!")

method handshake*(s: Secure,
                  conn: Connection,
                  initiator: bool): Future[SecureConn] {.async, base.} =
  doAssert(false, "Not implemented!")

proc handleConn(s: Secure,
                 conn: Connection,
                 initiator: bool): Future[Connection] {.async.} =
  var sconn = await s.handshake(conn, initiator)
  # mark connection bottom level transport direction
  # this is the safest place to do this
  # we require this information in for example gossipsub
  sconn.transportDir = if initiator: Direction.Out else: Direction.In

  proc cleanup() {.async.} =
    try:
      let futs = [conn.join(), sconn.join()]
      await futs[0] or futs[1]
      for f in futs:
        if not f.finished: await f.cancelAndWait() # cancel outstanding join()

      await allFuturesThrowing(
        sconn.close(), conn.close())
    except CancelledError:
      # This is top-level procedure which will work as separate task, so it
      # do not need to propagate CancelledError.
      discard
    except CatchableError as exc:
      debug "error cleaning up secure connection", err = exc.msg, sconn

  if not isNil(sconn):
    # All the errors are handled inside `cleanup()` procedure.
    asyncSpawn cleanup()

  return sconn

method init*(s: Secure) =
  procCall LPProtocol(s).init()

  proc handle(conn: Connection, proto: string) {.async.} =
    trace "handling connection upgrade", proto, conn
    try:
      # We don't need the result but we
      # definitely need to await the handshake
      discard await s.handleConn(conn, false)
      trace "connection secured", conn
    except CancelledError as exc:
      warn "securing connection canceled", conn
      await conn.close()
      raise exc
    except CatchableError as exc:
      warn "securing connection failed", err = exc.msg, conn
      await conn.close()
      raise exc

  s.handler = handle

method secure*(s: Secure,
               conn: Connection,
               initiator: bool):
               Future[Connection] {.base.} =
  s.handleConn(conn, initiator)

method readOnce*(s: SecureConn,
                 pbytes: pointer,
                 nbytes: int):
                 Future[int] {.async.} =
  doAssert(nbytes > 0, "nbytes must be positive integer")

  if s.isEof:
    raise newLPStreamEOFError()

  if s.buf.data().len() == 0:
    try:
      let buf = await s.readMessage() # Always returns >0 bytes or raises
      s.activity = true
      s.buf.add(buf)
    except LPStreamEOFError as err:
      s.isEof = true
      await s.close()
      raise err
    except CatchableError as err:
      debug "Error while reading message from secure connection, closing.",
        error = err.name,
        message = err.msg,
        connection = s
      await s.close()
      raise err

  var p = cast[ptr UncheckedArray[byte]](pbytes)
  return s.buf.consumeTo(toOpenArray(p, 0, nbytes - 1))
