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

logScope:
  topics = "libp2p tcptransport"

export transport

const
  TcpTransportTrackerName* = "libp2p.tcptransport"

type
  TcpTransport* = ref object of Transport
    server*: StreamServer
    clients: array[Direction, seq[StreamTransport]]
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

proc init*(
  T: typedesc[TcpTransport],
  flags: set[ServerFlags] = {},
  upgrade: Upgrade): T {.deprecated: "use .new".} =

  T.new(flags, upgrade)

proc new*(
  T: typedesc[TcpTransport],
  flags: set[ServerFlags] = {},
  upgrade: Upgrade): T =

  let transport = T(
    flags: flags,
    upgrader: upgrade
  )

  inc getTcpTransportTracker().opened
  return transport

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

  trace "Listening on", address = self.ma

method stop*(self: TcpTransport) {.async, gcsafe.} =
  ## stop the transport
  ##

  try:
    trace "Stopping TCP transport"
    await procCall Transport(self).stop() # call base

    checkFutures(
      await allFinished(
        self.clients[Direction.In].mapIt(it.closeWait()) &
        self.clients[Direction.Out].mapIt(it.closeWait())))

    # server can be nil
    if not isNil(self.server):
      await self.server.closeWait()

    self.server = nil
    trace "Transport stopped"
    inc getTcpTransportTracker().closed
  except CatchableError as exc:
    trace "Error shutting down tcp transport", exc = exc.msg

method accept*(self: TcpTransport): Future[Connection] {.async, gcsafe.} =
  ## accept a new TCP connection
  ##

  if not self.running:
    raise newTransportClosedError()

  try:
    let transp = await self.server.accept()
    return await self.connHandler(transp, Direction.In)
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

method dial*(
  self: TcpTransport,
  address: MultiAddress): Future[Connection] {.async, gcsafe.} =
  ## dial a peer
  ##

  trace "Dialing remote peer", address = $address

  let transp = await connect(address)
  return await self.connHandler(transp, Direction.Out)

method handles*(t: TcpTransport, address: MultiAddress): bool {.gcsafe.} =
  if procCall Transport(t).handles(address):
    if address.protocols.isOk:
      return address.protocols
        .get()
        .filterIt(
          it == multiCodec("tcp")
        ).len > 0
