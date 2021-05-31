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
    await transport.start(address)

  teardown:
    await transport.stop()

  template incoming(stream, body) =
    let session = await transport.accept()
    let stream = await session.getStream()
    body
    await stream.close()
    await session.close()

  template outgoing(stream, body) =
    let connection = await dial(initTAddress(address).tryGet)
    let stream = await connection.openStream()
    body
    await stream.close()
    await connection.waitClosed()

  test "handles QUIC addresses":
    let tcpAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/45894").get()
    check transport.handles(address) == true
    check transport.handles(tcpAddress) == false

  test "handles incoming stream":
    const message = "test".toBytes()

    proc read {.async.} =
      incoming stream:
        var buffer: array[message.len, byte]
        await stream.readExactly(addr buffer[0], buffer.len)
        check @buffer == message

    let reading = read()

    outgoing stream:
      await stream.write(message)

    await reading

