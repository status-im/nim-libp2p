import pkg/asynctest
import pkg/chronos
import pkg/quic
import pkg/stew/byteutils
import ../libp2p/transports/quictransport
import ../libp2p/multiaddress
import ../libp2p/wire

suite "QUIC transport":

  let address = MultiAddress.init("/ip4/127.0.0.1/udp/45894/quic").get()
  var transport: QuicTransport

  setup:
    transport = QuicTransport.new()

  test "handles QUIC addresses":
    let tcpAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/45894").get()
    check transport.handles(address) == true
    check transport.handles(tcpAddress) == false


  test "handles incoming stream":

    const message = "test".toBytes()

    proc read {.async.} =
      await transport.start(address)
      let session = await transport.accept()
      let stream = await session.getStream()
      var buffer: array[message.len, byte]
      await stream.readExactly(addr buffer[0], buffer.len)
      check @buffer == message
      await stream.close()
      await session.close()

    let reading = read()

    let connection = await dial(initTAddress(address).tryGet)
    let stream = await connection.openStream()
    await stream.write(message)
    await stream.close()
    await connection.waitClosed()

    await reading