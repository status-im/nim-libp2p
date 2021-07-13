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

proc commonTransportTest*(transportType: typedesc[Transport], ma: string) =
  suite $transportType & " common":
    teardown:
      checkTrackers()
    asyncTest "e2e: handle write":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.accept()
        await conn.write("Hello!")
        await conn.close()

      let handlerWait = acceptHandler()

      let transport2: transportType = transportType.new(upgrade = Upgrade())
      let conn = await transport2.dial(transport1.ma)
      var msg = newSeq[byte](6)
      await conn.readExactly(addr msg[0], 6)

      await conn.close() #for some protocols, closing requires actively, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

      check string.fromBytes(msg) == "Hello!"

    asyncTest "e2e: handle read":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()
      let transport1: transportType = transportType.new(upgrade = Upgrade())
      asyncSpawn transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.accept()
        var msg = newSeq[byte](6)
        await conn.readExactly(addr msg[0], 6)
        check string.fromBytes(msg) == "Hello!"
        await conn.close()

      let handlerWait = acceptHandler()

      let transport2: transportType = transportType.new(upgrade = Upgrade())
      let conn = await transport2.dial(transport1.ma)
      await conn.write("Hello!")

      await conn.close() #for some protocols, closing requires actively, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle dial cancellation":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      let transport2: transportType = transportType.new(upgrade = Upgrade())
      let cancellation = transport2.dial(transport1.ma)

      await cancellation.cancelAndWait()
      check cancellation.cancelled

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle accept cancellation":
      let ma: MultiAddress = Multiaddress.init(ma).tryGet()

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      let acceptHandler = transport1.accept()
      await acceptHandler.cancelAndWait()
      check acceptHandler.cancelled

      await transport1.stop()
