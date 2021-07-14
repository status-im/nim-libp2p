{.used.}

import sequtils
import chronos, stew/byteutils
import ../libp2p/[stream/connection,
                  transports/transport,
                  upgrademngrs/upgrade,
                  multiaddress,
                  errors,
                  wire]

import ./helpers

type TransportProvider* = proc(): Transport {.gcsafe.}

proc commonTransportTest*(name: string, prov: TransportProvider, ma: string) =
  suite name & " common tests":
    teardown:
      checkTrackers()

    asyncTest "can handle local address":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()
      let transport1 = prov()
      await transport1.start(ma)
      check transport1.handles(transport1.ma)
      await transport1.stop()

    asyncTest "e2e: handle write":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1 = prov()
      await transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.accept()
        await conn.write("Hello!")
        await conn.close()

      let handlerWait = acceptHandler()

      let transport2 = prov()
      let conn = await transport2.dial(transport1.ma)
      var msg = newSeq[byte](6)
      await conn.readExactly(addr msg[0], 6)

      await conn.close() #for some protocols, closing requires actively reading, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

      check string.fromBytes(msg) == "Hello!"

    asyncTest "e2e: handle read":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()
      let transport1 = prov()
      asyncSpawn transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.accept()
        var msg = newSeq[byte](6)
        await conn.readExactly(addr msg[0], 6)
        check string.fromBytes(msg) == "Hello!"
        await conn.close()

      let handlerWait = acceptHandler()

      let transport2 = prov()
      let conn = await transport2.dial(transport1.ma)
      await conn.write("Hello!")

      await conn.close() #for some protocols, closing requires actively reading, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle dial cancellation":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1 = prov()
      await transport1.start(ma)

      let transport2 = prov()
      let cancellation = transport2.dial(transport1.ma)

      await cancellation.cancelAndWait()
      check cancellation.cancelled

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle accept cancellation":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1 = prov()
      await transport1.start(ma)

      let acceptHandler = transport1.accept()
      await acceptHandler.cancelAndWait()
      check acceptHandler.cancelled

      await transport1.stop()

    asyncTest "e2e: stopping transport kills connections":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1 = prov()
      await transport1.start(ma)

      let transport2 = prov()

      let acceptHandler = transport1.accept()
      let conn = await transport2.dial(transport1.ma)
      let serverConn = await acceptHandler

      #For user-space protocols, we need to read to allow
      #the other party to disconnect
      let clientRead = conn.readLp(1024)
      let serverRead = serverConn.readLp(1024)

      await transport1.stop()
      check serverConn.closed()

      await transport2.stop()
      check conn.closed()

      #will throw EOF
      try: discard await clientRead: except: discard
      try: discard await serverRead: except: discard
