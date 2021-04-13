import pkg/chronos
import pkg/quic
import ../multiaddress
import ../multicodec
import ../stream/connection
import ./transport

export multiaddress
export multicodec
export connection
export transport

type
  QuicTransport* = ref object of Transport
    listener: Listener
  P2PConnection = connection.Connection
  QuicConnection = quic.Connection

func new*(_: type QuicTransport): QuicTransport =
  QuicTransport()

method handles*(transport: QuicTransport, address: MultiAddress): bool =
  if not procCall Transport(transport).handles(address):
    return false
  QUIC.match(address)
