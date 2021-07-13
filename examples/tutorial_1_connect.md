Hi all, welcome to the first article of the nim-libp2p's tutorial series!

_This tutorial is for everyone who is interested in building peer-to-peer chatting applications. No Nim programming experience is needed._

To give you a quick overview, **Nim** is the programming language we are using and **nim-libp2p** is the Nim implementation of [libp2p](https://libp2p.io/), a modular library that enables the development of peer-to-peer network applications.

Hope you'll find it helpful in your journey of learning. Happy coding! ;)

# Before you start
The only prerequisite here is [Nim](https://nim-lang.org/), the programming language with a Python-like syntax and a performance similar to C. Detailed information can be found [here](https://nim-lang.org/docs/tut1.html).

Install Nim via their official website: [https://nim-lang.org/install.html](https://nim-lang.org/install.html)  
Check Nim's installation via `nim --version` and its package manager Nimble via `nimble --version`.

You can now install the latest version of `nim-libp2p`:
```bash
nimble install libp2p@#master
```

# A simple ping application
We'll start by creating a simple application, which is starting two libp2p [switch](https://docs.libp2p.io/concepts/stream-multiplexing/#switch-swarm), and pinging each other using the [Ping](https://docs.libp2p.io/concepts/protocols/#ping) protocol.

Let's create a `part1.nim`, and import our dependencies:
```nim
import bearssl
import chronos

import libp2p
import libp2p/protocols/ping
```
[bearssl](https://github.com/status-im/nim-bearssl) is used as a [cryptographic pseudorandom number generator](https://en.wikipedia.org/wiki/Cryptographically-secure_pseudorandom_number_generator)  
[chronos](https://github.com/status-im/nim-chronos) the asynchronous framework used by `nim-libp2p`

Next, we'll create an helper procedure to create our switches. A switch needs a bit of configuration, and it will be easier to do this configuration only once:
```nim
proc createSwitch(ma: MultiAddress, rng: ref BrHmacDrbgContext): Switch =
  var switch = SwitchBuilder
    .new()
    .withRng(rng)       # Give the application RNG
    .withAddress(ma)    # Our local address(es)
    .withTcpTransport() # Use TCP as transport
    .withMplex()        # Use Mplex as muxer
    .withNoise()        # Use Noise as secure manager
    .build()

  return switch
```
This will create a switch using [Mplex](https://docs.libp2p.io/concepts/stream-multiplexing/) as a multiplexer, Noise to secure the communication, and TCP as an underlying transport.

You can of course tweak this, to use a different or multiple transport, or tweak the configuration of Mplex and Noise, but this is some sane defaults that we'll use going forward.


Let's now start to create our main procedure:
```nim
proc main() {.async, gcsafe.} =
  let
    rng = newRng()
    localAddress = MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()
    pingProtocol = Ping.new(rng=rng)
```
We created some variables that we'll need for the rest of the application: the global `rng` instance, our `localAddress`, and an instance of the `Ping` protocol.  
The address is in the [MultiAddress](https://github.com/multiformats/multiaddr) format. The port `0` means "take any port available".

`tryGet` is procedure which is part of the [nim-result](https://github.com/arnetheduck/nim-result/), that will throw an exception if the supplied MultiAddress is not valid.

We can now create our two switches:
```nim
  let
    switch1 = createSwitch(localAddress, rng)
    switch2 = createSwitch(localAddress, rng)
    
  switch1.mount(pingProtocol)

  let
    switch1Fut = await switch1.start()
    switch2Fut = await switch2.start()
```
We've **mounted** the `pingProtocol` on our first switch. This means that the first switch will actually listen for any ping requests coming in, and handle them accordingly.

Now that we've started the nodes, they are listening for incoming peers.  
We can find out which port was attributed, and the resulting local addresses, by using `switch1.peerInfo.addrs`.

We'll **dial** the first switch from the second one, by specifying it's **Peer ID**, it's **MultiAddress** and the **`Ping` protocol codec**:
```nim
  let conn = await switch2.dial(switch1.peerInfo.peerId, switch1.peerInfo.addrs, PingCodec)
```
We now have a `Ping` connection setup between the second and the first switch, we can use it to actually ping the node:
```nim
  # ping the other node and echo the ping duration
  echo "ping: ", await pingProtocol.ping(conn)

  # We must close the connection ourselves when we're done with it
  await conn.close()
```

And that's it! Just a little bit of cleanup: shutting down the switches, waiting for them to stop, and we'll call our `main` procedure:
```nim
  await allFutures(switch1.stop(), switch2.stop()) # close connections and shutdown all transports
  await allFutures(switch1Fut & switch2Fut) # wait for all transports to shutdown
  
waitFor(main())
```

You can now run this program using `nim c -r part1.nim`, and you should see the dialing sequence, ending with a ping output.

In the [next tutorial](tutorial_2_customproto.md), we'll look at how to create our own custom protocol.
