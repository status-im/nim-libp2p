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
      let ma = @[Multiaddress.init(ma).tryGet()]

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      proc acceptHandler() {.async, gcsafe.} =
        let conn = await transport1.accept()
        await conn.write("Hello!")
        await conn.close()

      let handlerWait = acceptHandler()

      let transport2: transportType = transportType.new(upgrade = Upgrade())
      let conn = await transport2.dial(transport1.addrs[0])
      var msg = newSeq[byte](6)
      await conn.readExactly(addr msg[0], 6)

      await conn.close() #for some protocols, closing requires actively, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

      check string.fromBytes(msg) == "Hello!"

    asyncTest "e2e: handle read":
      let ma = @[Multiaddress.init(ma).tryGet()]
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
      let conn = await transport2.dial(transport1.addrs[0])
      await conn.write("Hello!")

      await conn.close() #for some protocols, closing requires actively, so we must close here
      await handlerWait.wait(1.seconds) # when no issues will not wait that long!

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle dial cancellation":
      let ma = @[Multiaddress.init(ma).tryGet()]

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      let transport2: transportType = transportType.new(upgrade = Upgrade())
      let cancellation = transport2.dial(transport1.addrs[0])

      await cancellation.cancelAndWait()
      check cancellation.cancelled

      await transport2.stop()
      await transport1.stop()

    asyncTest "e2e: handle accept cancellation":
      let ma = @[Multiaddress.init(ma).tryGet()]

      let transport1: transportType = transportType.new(upgrade = Upgrade())
      await transport1.start(ma)

      let acceptHandler = transport1.accept()
      await acceptHandler.cancelAndWait()
      check acceptHandler.cancelled

      await transport1.stop()

    asyncTest "e2e should allow multiple local addresses":
      let addrs = @[MultiAddress.init(ma).tryGet(),
                    MultiAddress.init(ma).tryGet()]


      let transport1 = transportType.new(upgrade = Upgrade())
      await transport1.start(addrs)

      proc acceptHandler() {.async, gcsafe.} =
        while true:
          let conn = await transport1.accept()
          await conn.write("Hello!")
          await conn.close()

      let handlerWait = acceptHandler()

      check transport1.addrs.len == 2
      check transport1.addrs[0] != transport1.addrs[1]

      var msg = newSeq[byte](6)

      proc client(ma: MultiAddress) {.async.} =
        let conn1 = await transport1.dial(ma)
        await conn1.readExactly(addr msg[0], 6)
        check string.fromBytes(msg) == "Hello!"
        await conn1.close()

      #Dial the same server multiple time in a row
      await client(transport1.addrs[0])
      await client(transport1.addrs[0])
      await client(transport1.addrs[0])

      #Dial the same server on different addresses
      await client(transport1.addrs[1])
      await client(transport1.addrs[0])
      await client(transport1.addrs[1])

      #Cancel a dial
      let
        dial1 = transport1.dial(transport1.addrs[1])
        dial2 = transport1.dial(transport1.addrs[0])
      await dial1.cancelAndWait()
      await dial2.cancelAndWait()

      await handlerWait.cancelAndWait()
      await transport1.stop()
