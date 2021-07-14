## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[sequtils]
import chronos, chronicles
import transport,
       ../errors,
       ../wire,
       ../multicodec,
       ../multistream,
       ../connmanager,
       ../multiaddress,
       ../stream/connection,
       ../upgrademngrs/upgrade,
       websock/websock

logScope:
  topics = "libp2p wstransport"

export transport, websock

const
  WsTransportTrackerName* = "libp2p.wstransport"

type
  WsStream = ref object of Connection
    session: WSSession

proc init*(T: type WsStream,
           session: WSSession,
           dir: Direction,
           timeout = 10.minutes,
           observedAddr: MultiAddress = MultiAddress()): T =

  let stream = T(session: session,
             timeout: timeout,
             dir: dir,
             observedAddr: observedAddr)
  stream.initStream()
  return stream

method readOnce*(
  s: WsStream,
  pbytes: pointer,
  nbytes: int):
  Future[int] =
  return s.session.recv(pbytes, nbytes)

method write*(s: WsStream, msg: seq[byte]):
  Future[void] {.async.} =
  try:
    await s.session.send(msg, Opcode.Binary)
  except WSClosedError:
    raise newLPStreamEOFError()

method closeImpl*(s: WsStream): Future[void] {.async.} =
  await s.session.close()
  await procCall Connection(s).closeImpl()

type
  WsTransport* = ref object of Transport
    httpserver: HttpServer
    wsserver: WSServer
    connections: array[Direction, seq[WsStream]]

    tlsPrivateKey: TLSPrivateKey
    tlsCertificate: TLSCertificate
    tlsFlags: set[TLSFlags]


method start*(
  self: WsTransport,
  ma: MultiAddress) {.async.} =
  ## listen on the transport
  ##

  if self.running:
    trace "WS transport already running"
    return

  await procCall Transport(self).start(ma)
  trace "Starting WS transport"

  self.httpserver =
    if isNil(self.tlsPrivateKey):
      HttpServer.create(self.ma.initTAddress().tryGet())
    else:
      TlsHttpServer.create(self.ma.initTAddress().tryGet(),
        self.tlsPrivateKey,
        self.tlsCertificate)

  self.wsserver = WSServer.new()

  let codec = if isNil(self.tlsPrivateKey):
      MultiAddress.init("/ws")
    else:
      MultiAddress.init("/wss")
  # always get the resolved address in case we're bound to 0.0.0.0:0
  self.ma = MultiAddress.init(self.httpserver.sock.getLocalAddress()).tryGet() & codec.tryGet()
  self.running = true

  trace "Listening on", address = self.ma

method stop*(self: WsTransport) {.async, gcsafe.} =
  ## stop the transport
  ##

  self.running = false # mark stopped as soon as possible

  try:
    trace "Stopping WS transport"
    await procCall Transport(self).stop() # call base

    var toWait: seq[Future[void]]
    # server can be nil
    if not isNil(self.httpserver):
      self.httpserver.stop()
      toWait.add(self.httpserver.closeWait())

    for conn in self.connections[Direction.In] &
                self.connections[Direction.Out]:
      toWait.add(conn.close())

    await allFutures(toWait)
    self.httpserver = nil
    trace "Transport stopped"
  except CatchableError as exc:
    trace "Error shutting down ws transport", exc = exc.msg

proc trackConnection(self: WsTransport, conn: WsStream, dir: Direction) =
  self.connections[dir].add(conn)
  proc onClose() {.async.} =
    await conn.session.stream.reader.join()
    self.connections[dir].keepItIf(it != conn)
    trace "Cleaned up client"
  asyncSpawn onClose()

method accept*(self: WsTransport): Future[Connection] {.async, gcsafe.} =
  ## accept a new WS connection
  ##

  if not self.running:
    raise newTransportClosedError()

  try:
    let
      transp = await self.httpserver.accept()
      wstransp = await self.wsserver.handleRequest(transp)
      stream = WsStream.init(wstransp, Direction.In)
    self.trackConnection(stream, Direction.In)
    return stream
  except TransportOsError as exc:
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
  self: WsTransport,
  address: MultiAddress): Future[Connection] {.async, gcsafe.} =
  ## dial a peer
  ##

  trace "Dialing remote peer", address = $address

  let
    secure = WSS.match(address)
    transp = await WebSocket.connect(address.initTAddress().tryGet(), "", secure=secure, flags=self.tlsFlags)
    stream = WsStream.init(transp, Direction.Out)

  self.trackConnection(stream, Direction.Out)
  return stream

method handles*(t: WsTransport, address: MultiAddress): bool {.gcsafe.} =
  if procCall Transport(t).handles(address):
    if address.protocols.isOk:
      return WebSockets.match(address)

proc new*(T: typedesc[WsTransport], upgrade: Upgrade): T =

  ## Standard WsTransport
  T(
    upgrader: upgrade)

proc new*(T: typedesc[WsTransport],
  tlsPrivateKey: TLSPrivateKey,
  tlsCertificate: TLSCertificate,
  upgrade: Upgrade,
  tlsFlags: set[TLSFlags] = {}): T =

  ## Secure WsTransport
  T(
    upgrader: upgrade,
    tlsPrivateKey: tlsPrivateKey,
    tlsCertificate: tlsCertificate,
    tlsFlags: tlsFlags)
