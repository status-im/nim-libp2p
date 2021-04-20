## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[oids, sequtils]
import chronos, chronicles
import transport,
       ../errors,
       ../wire,
       ../multicodec,
       ../multistream,
       ../connmanager,
       ../multiaddress,
       ../stream/connection,
       ../stream/chronosstream,
       ../upgrademngrs/upgrade
import ./tcpsession

logScope:
  topics = "libp2p tcptransport"

export transport

const
  TcpTransportTrackerName* = "libp2p.tcptransport"

type
  TcpTransport* = ref object of Transport
    server*: StreamServer
    clients: array[Direction, seq[StreamTransport]]
    sessions: array[Direction, seq[TcpSession]]
    flags: set[ServerFlags]

  TcpTransportTracker* = ref object of TrackerBase
    opened*: uint64
    closed*: uint64

proc setupTcpTransportTracker(): TcpTransportTracker {.gcsafe, raises: [Defect].}

proc getTcpTransportTracker(): TcpTransportTracker {.gcsafe.} =
  result = cast[TcpTransportTracker](getTracker(TcpTransportTrackerName))
  if isNil(result):
    result = setupTcpTransportTracker()

proc dumpTracking(): string {.gcsafe.} =
  var tracker = getTcpTransportTracker()
  result = "Opened tcp transports: " & $tracker.opened & "\n" &
           "Closed tcp transports: " & $tracker.closed

proc leakTransport(): bool {.gcsafe.} =
  var tracker = getTcpTransportTracker()
  result = (tracker.opened != tracker.closed)

proc setupTcpTransportTracker(): TcpTransportTracker =
  result = new TcpTransportTracker
  result.opened = 0
  result.closed = 0
  result.dump = dumpTracking
  result.isLeaked = leakTransport
  addTracker(TcpTransportTrackerName, result)

proc sessionHandler*(self: TcpTransport,
                     client: StreamTransport,
                     dir: Direction): Future[Session] {.async.} =
  var observedAddr: MultiAddress = MultiAddress()
  try:
    observedAddr = MultiAddress.init(client.remoteAddress).tryGet()
  except CatchableError as exc:
    trace "Connection setup failed", exc = exc.msg
    if not(isNil(client) and client.closed):
      await client.closeWait()
      raise exc

  trace "Handling tcp connection", address = $observedAddr,
                                   dir = $dir,
                                   sessions = self.sessions[Direction.In].len +
                                   self.sessions[Direction.Out].len

  let session = TcpSession.new(client, dir, observedAddr)

  proc onClose() {.async.} =
    try:
      let futs = @[client.join(), session.join()]
      await futs[0] or futs[1]
      for f in futs:
        if not f.finished: await f.cancelAndWait() # cancel outstanding join()

      trace "Cleaning up client", addrs = $client.remoteAddress,
                                  session

      self.sessions[dir].keepItIf( it != session )
      await allFuturesThrowing(
        session.close(), client.closeWait())

      trace "Cleaned up client", addrs = $client.remoteAddress,
                                 session

    except CatchableError as exc:
      let useExc {.used.} = exc
      debug "Error cleaning up client", errMsg = exc.msg, session

  self.sessions[dir].add(session)
  asyncSpawn onClose()

  return session

proc connHandler*(self: TcpTransport,
                  client: StreamTransport,
                  dir: Direction): Future[Connection] {.async.} =
  var observedAddr: MultiAddress = MultiAddress()
  try:
    observedAddr = MultiAddress.init(client.remoteAddress).tryGet()
  except CatchableError as exc:
    trace "Connection setup failed", exc = exc.msg
    if not(isNil(client) and client.closed):
      await client.closeWait()
      raise exc

  trace "Handling tcp connection", address = $observedAddr,
                                   dir = $dir,
                                   clients = self.clients[Direction.In].len +
                                   self.clients[Direction.Out].len

  let conn = Connection(
    ChronosStream.init(
      client = client,
      dir = dir,
      observedAddr = observedAddr
    ))

  proc onClose() {.async.} =
    try:
      let futs = @[client.join(), conn.join()]
      await futs[0] or futs[1]
      for f in futs:
        if not f.finished: await f.cancelAndWait() # cancel outstanding join()

      trace "Cleaning up client", addrs = $client.remoteAddress,
                                  conn

      self.clients[dir].keepItIf( it != client )
      await allFuturesThrowing(
        conn.close(), client.closeWait())

      trace "Cleaned up client", addrs = $client.remoteAddress,
                                 conn

    except CatchableError as exc:
      let useExc {.used.} = exc
      debug "Error cleaning up client", errMsg = exc.msg, conn

  self.clients[dir].add(client)
  asyncSpawn onClose()

  return conn

func init*(
  T: type TcpTransport,
  flags: set[ServerFlags] = {},
  upgrade: Upgrade): T =

  result = T(
    flags: flags,
    upgrader: upgrade
  )

  result.initTransport()

method initTransport*(self: TcpTransport) =
  self.multicodec = multiCodec("tcp")
  inc getTcpTransportTracker().opened

method start*(
  self: TcpTransport,
  ma: MultiAddress) {.async.} =
  ## listen on the transport
  ##

  if self.running:
    trace "TCP transport already running"
    return

  await procCall Transport(self).start(ma)
  trace "Starting TCP transport"

  self.server = createStreamServer(
    ma = self.ma,
    flags = self.flags,
    udata = self)

  # always get the resolved address in case we're bound to 0.0.0.0:0
  self.ma = MultiAddress.init(self.server.sock.getLocalAddress()).tryGet()
  self.running = true

  trace "Listening on", address = self.ma

method stop*(self: TcpTransport) {.async, gcsafe.} =
  ## stop the transport
  ##

  self.running = false # mark stopped as soon as possible

  try:
    trace "Stopping TCP transport"
    await procCall Transport(self).stop() # call base

    checkFutures(
      await allFinished(
        self.clients[Direction.In].mapIt(it.closeWait()) &
        self.clients[Direction.Out].mapIt(it.closeWait())))

    checkFutures(
      await allFinished(
        self.sessions[Direction.In].mapIt(it.close()) &
        self.sessions[Direction.Out].mapIt(it.close())))

    # server can be nil
    if not isNil(self.server):
      await self.server.closeWait()

    self.server = nil
    trace "Transport stopped"
    inc getTcpTransportTracker().closed
  except CatchableError as exc:
    trace "Error shutting down tcp transport", exc = exc.msg

method upgradeIncoming*(
  self: TcpTransport,
  conn: Connection): Future[void] {.gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  self.upgrader.upgradeIncoming(conn)

method upgradeOutgoing*(
  self: TcpTransport,
  conn: Connection): Future[Connection] {.gcsafe.} =
  ## base upgrade method that the transport uses to perform
  ## transport specific upgrades
  ##

  self.upgrader.upgradeOutgoing(conn)

method accept*(self: TcpTransport): Future[Session] {.async.} =
  if not self.running:
    raise newTransportClosedError()

  try:
    let transp = await self.server.accept()
    return await self.sessionHandler(transp, Direction.In)
  except TransportOsError as exc:
    # TODO: it doesn't sound like all OS errors
    # can  be ignored, we should re-raise those
    # that can'self.
    debug "OS Error", exc = exc.msg
  except TransportTooManyError as exc:
    debug "Too many files opened", exc = exc.msg
  except TransportUseClosedError as exc:
    debug "Server was closed", exc = exc.msg
    raise newTransportClosedError(exc)
  except CatchableError as exc:
    warn "Unexpected error creating connection", exc = exc.msg
    raise exc

method dial*(self: TcpTransport,
             address: MultiAddress): Future[Session] {.async.} =
  trace "Dialing remote peer", address = $address

  let transp = await connect(address)
  return await self.sessionHandler(transp, Direction.Out)

method handles*(t: TcpTransport, address: MultiAddress): bool {.gcsafe.} =
  if procCall Transport(t).handles(address):
    if address.protocols.isOk:
      return address.protocols
        .get()
        .filterIt(
          it == multiCodec("tcp")
        ).len > 0
