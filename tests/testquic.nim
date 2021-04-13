import pkg/asynctest
import pkg/chronos
import ../libp2p/transports/quictransport
import ../libp2p/multiaddress

suite "QUIC transport":

  let address = MultiAddress.init("/ip4/127.0.0.1/udp/45894/quic").get()
  var transport: QuicTransport

  setup:
    transport = QuicTransport.new()

  test "handles QUIC addresses":
    let tcpAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/45894").get()
    check transport.handles(address) == true
    check transport.handles(tcpAddress) == false
