import bearssl
import chronos         # an efficient library for async
import stew/byteutils  # various utils
import ../libp2p       # when installed through nimble, just use `import libp2p`

##
# Create our custom protocol
##
const TestCodec = "/test/proto/1.0.0" # custom protocol string identifier

type
  TestProto = ref object of LPProtocol # declare a custom protocol

proc new(T: typedesc[TestProto]): T =

  # every incoming connections will in handled in this closure
  proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
    echo "Got from remote - ", string.fromBytes(await conn.readLp(1024))
    await conn.writeLp("Roger p2p!")

    # We must close the connections ourselves when we're done with it
    await conn.close()

  return T(codecs: @[TestCodec], handler: handle)

##
# Helper to create a switch/node
##
proc createSwitch(ma: MultiAddress, rng: ref BrHmacDrbgContext): Switch =
  var switch = SwitchBuilder
    .new()
    .withRng(rng)       # Give the application RNG
    .withAddress(ma)    # Our local address(es)
    .withTcpTransport() # Use TCP as transport
    .withMplex()        # Use Mplex as muxer
    .withNoise()        # Use Noise as secure manager
    .build()

  result = switch

##
# The actual application
##
proc main() {.async, gcsafe.} =
  let
    rng = newRng() # Single random number source for the whole application
    # port 0 will take a random available port
    # `tryGet` will throw an exception if the Multiaddress failed
    # (for instance, if the address is not well formatted)
    ma1 = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
    ma2 = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

  # setup the custom proto
  let testProto = TestProto.new()

  # setup the two nodes
  let
    switch1 = createSwitch(ma1, rng) #Create the two switches
    switch2 = createSwitch(ma2, rng)

  # mount the proto on switch1
  # the node will now listen for this proto
  # and call the handler everytime a client request it
  switch1.mount(testProto)

  # Start the nodes. This will start the transports
  # and listen on each local addresses
  let
    switch1Fut = await switch1.start()
    switch2Fut = await switch2.start()

  # the node addrs is populated with it's
  # actual port during the start

  # use the second node to dial the first node
  # using the first node peerid and address
  # and specify our custom protocol codec
  let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, TestCodec)

  # conn is now a fully setup connection, we talk directly to the node1 custom protocol handler
  await conn.writeLp("Hello p2p!") # writeLp send a length prefixed buffer over the wire

  # readLp reads length prefixed bytes and returns a buffer without the prefix
  echo "Remote responded with - ", string.fromBytes(await conn.readLp(1024))

  # We must close the connection ourselves when we're done with it
  await conn.close()

  await allFutures(switch1.stop(), switch2.stop()) # close connections and shutdown all transports
  await allFutures(switch1Fut & switch2Fut) # wait for all transports to shutdown

waitFor(main())
