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
    servers*: seq[StreamServer]
    clients: array[Direction, seq[StreamTransport]]
    flags: set[ServerFlags]
    acceptFuts: seq[Future[StreamTransport]]

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
    upgrader: upgrade)

  inc getTcpTransportTracker().opened
  return transport

method start*(
  self: TcpTransport,
  addrs: seq[MultiAddress]) {.async.} =
  ## listen on the transport
  ##

  if self.running:
    warn "TCP transport already running"
    return

  await procCall Transport(self).start(addrs)
  trace "Starting TCP transport"

  for i, ma in addrs:
    if not self.handles(ma):
      trace "Invalid address detected, skipping!", address = ma
      continue

    let server = createStreamServer(
      ma = ma,
      flags = self.flags,
      udata = self)

    # always get the resolved address in case we're bound to 0.0.0.0:0
    self.addrs[i] = MultiAddress.init(
      server.sock.getLocalAddress()
    ).tryGet()

    self.servers &= server

    trace "Listening on", address = ma

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

    var toWait: seq[Future[void]]
    for fut in self.acceptFuts:
      if not fut.finished:
        toWait.add(fut.cancelAndWait())
      elif fut.done:
        toWait.add(fut.read().closeWait())

    for server in self.servers:
      server.close()
      toWait.add(server.join())

    discard await allFinished(toWait)

    self.servers = @[]

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
    if self.acceptFuts.len <= 0:
      self.acceptFuts = self.servers.mapIt(it.accept())

    if self.acceptFuts.len <= 0:
      return

    let
      finished = await one(self.acceptFuts)
      index = self.acceptFuts.find(finished)

    self.acceptFuts[index] = self.servers[index].accept()

    let transp = await finished
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
  except CancelledError as exc:
    raise
  except CatchableError as exc:
    warn "Unexpected error accepting connection", exc = exc.msg
    raise exc

method dial*(
  self: TcpTransport,
  address: MultiAddress): Future[Connection] {.async, gcsafe.} =
  ## dial a peer
  ##

  trace "Dialing remote peer", address = $address

  let transp = await connect(address)
  try:
    return await self.connHandler(transp, Direction.Out)
  except CatchableError as err:
    await transp.closeWait()
    raise err

method handles*(t: TcpTransport, address: MultiAddress): bool {.gcsafe.} =
  if procCall Transport(t).handles(address):
    if address.protocols.isOk:
      return address.protocols
        .get()
        .filterIt(
          it == multiCodec("tcp")
        ).len > 0
