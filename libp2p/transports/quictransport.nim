import pkg/chronos
import pkg/quic
import ../multiaddress
import ../multicodec
import ../stream/connection
import ../wire
import ./transport

export multiaddress
export multicodec
export connection
export transport

type
  P2PConnection = connection.Connection
  QuicConnection = quic.Connection

type
  QuicTransport* = ref object of Transport
    listener: Listener
  QuicSession* = ref object of Session
    connection: QuicConnection
  QuicStream* = ref object of P2PConnection
    stream: Stream
    cached: seq[byte]

func new*(_: type QuicTransport): QuicTransport =
  QuicTransport()

method handles*(transport: QuicTransport, address: MultiAddress): bool =
  if not procCall Transport(transport).handles(address):
    return false
  QUIC.match(address)

method start*(transport: QuicTransport, address: MultiAddress) {.async.} =
  doAssert transport.listener.isNil, "start() already called"
  transport.listener = listen(initTAddress(address).tryGet)
  await procCall Transport(transport).start(address)

method accept*(transport: QuicTransport): Future[Session] {.async.} =
  doAssert not transport.listener.isNil, "call start() before calling accept()"
  let connection = await transport.listener.accept()
  return QuicSession(connection: connection)

method getStream*(session: QuicSession): Future[P2PConnection] {.async.} =
  let stream = await session.connection.incomingStream()
  return QuicStream(stream: stream)

method readOnce*(stream: QuicStream,
                 pbytes: pointer,
                 nbytes: int): Future[int] {.async.} =
  if stream.cached.len == 0:
    stream.cached = await stream.stream.read()
  if stream.cached.len <= nbytes:
    copyMem(pbytes, addr stream.cached[0], stream.cached.len)
    return stream.cached.len
  else:
    copyMem(pbytes, addr stream.cached[0], nbytes)
    stream.cached = stream.cached[nbytes..^1]
    return nbytes

method close*(stream: QuicStream) {.async.} =
  await stream.stream.close()

method close*(session: QuicSession) {.async.} =
  await session.connection.close()