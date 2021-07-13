when not(compileOption("threads")):
  {.fatal: "Please, compile this program with the --threads:on option!".}

import
  strformat, strutils, bearssl,
  stew/byteutils,
  chronos,
  ../libp2p

const DefaultAddr = "/ip4/127.0.0.1/tcp/0"

const Help = """
  Commands: /[?|help|connect|disconnect|exit]
  help: Prints this help
  connect: dials a remote peer
  disconnect: ends current session
  exit: closes the chat
"""

type
  Chat = ref object
    switch: Switch          # a single entry point for dialing and listening to peer
    stdinReader: StreamTransport # transport streams between read & write file descriptor
    conn: Connection        # connection to the other peer
    connected: bool         # if the node is connected to another peer

##
# Stdout helpers, to write the prompt
##
proc writePrompt(c: Chat) =
  if c.connected:
    stdout.write '\r' & $c.switch.peerInfo.peerId & ": "
    stdout.flushFile()

proc writeStdout(c: Chat, str: string) =
  echo '\r' & str
  c.writePrompt()

##
# Chat Protocol
##
const ChatCodec = "/nim-libp2p/chat/1.0.0"

type
  ChatProto = ref object of LPProtocol

proc new(T: typedesc[ChatProto], c: Chat): T =
  let chatproto = T()

  # create handler for incoming connection
  proc handle(stream: Connection, proto: string) {.async.} =
    if c.connected and not c.conn.closed:
      c.writeStdout "a chat session is already in progress - refusing incoming peer!"
      await stream.close()
    else:
      await c.handlePeer(stream)
      await stream.close()

  # assign the new handler
  chatproto.handler = handle
  chatproto.codec = ChatCodec
  return chatproto

##
# Chat application
##
proc handlePeer(c: Chat, conn: Connection) {.async.} =
  # Handle a peer (incoming or outgoing)
  try:
    c.conn = conn
    c.connected = true
    c.writeStdout $conn.peerInfo.peerId & " connected"

    # Read loop
    while true:
      let
        strData = await conn.readLp(1024)
        str = string.fromBytes(strData)
      c.writeStdout $conn.peerInfo.peerId & ": " & $str

  except LPStreamEOFError:
    c.writeStdout $conn.peerInfo.peerId & " disconnected"

proc dialPeer(c: Chat, address: string) {.async.} =
  # Parse and dial address
  let
    multiAddr = MultiAddress.init(address).tryGet()
    # split the peerId part /p2p/...
    peerIdBytes = multiAddr[multiCodec("p2p")]
      .tryGet()
      .protoAddress()
      .tryGet()
    remotePeer = PeerID.init(peerIdBytes).tryGet()
    # split the wire address
    ip4Addr = multiAddr[multiCodec("ip4")].tryGet()
    tcpAddr = multiAddr[multiCodec("tcp")].tryGet()
    wireAddr = ip4Addr & tcpAddr

  echo &"dialing peer: {multiAddr}"
  asyncSpawn c.handlePeer(await c.switch.dial(remotePeer, @[wireAddr], ChatCodec))

proc readLoop(c: Chat) {.async.} =
  while true:
    if not c.connected:
      echo "type an address or wait for a connection:"
      echo "type /[help|?] for help"

    c.writePrompt()

    let line = await c.stdinReader.readLine()
    if line.startsWith("/help") or line.startsWith("/?"):
      echo Help
      continue

    if line.startsWith("/disconnect"):
      c.writeStdout "Ending current session"
      if c.connected and c.conn.closed.not:
        await c.conn.close()
      c.connected = false
    elif line.startsWith("/connect"):
      c.writeStdout "enter address of remote peer"
      let address = await c.stdinReader.readLine()
      if address.len > 0:
        await c.dialPeer(address)

    elif line.startsWith("/exit"):
      if c.connected and c.conn.closed.not:
        await c.conn.close()
        c.connected = false

      await c.switch.stop()
      c.writeStdout "quitting..."
      quit(0)
    else:
      if c.connected:
        await c.conn.writeLp(line)
      else:
        try:
          if line.startsWith("/") and "p2p" in line:
            await c.dialPeer(line)
        except:
          echo &"unable to dial remote peer {line}"
          echo getCurrentExceptionMsg()

proc readInput(wfd: AsyncFD) {.thread.} =
  ## This thread performs reading from `stdin` and sends data over
  ## pipe to main thread.
  let transp = fromPipe(wfd)

  while true:
    let line = stdin.readLine()
    discard waitFor transp.write(line & "\r\n")

proc main() {.async.} =
  let
    rng = newRng() # Single random number source for the whole application

    # Pipe to read stdin from main thread
    (rfd, wfd) = createAsyncPipe()
    stdinReader = fromPipe(rfd)

  var thread: Thread[AsyncFD]
  thread.createThread(readInput, wfd)

  var localAddress = MultiAddress.init(DefaultAddr).tryGet()

  var switch = SwitchBuilder
    .new()
    .withRng(rng)       # Give the application RNG
    .withAddress(localAddress)
    .withTcpTransport() # Use TCP as transport
    .withMplex()        # Use Mplex as muxer
    .withNoise()        # Use Noise as secure manager
    .build()

  let chat = Chat(
    switch: switch,
    stdinReader: stdinReader)

  switch.mount(ChatProto.new(chat))

  let libp2pFuts = await switch.start()

  let id = $switch.peerInfo.peerId
  echo "PeerID: " & id
  echo "listening on: "
  for a in switch.peerInfo.addrs:
    echo &"{a}/p2p/{id}"

  await chat.readLoop()
  await allFuturesThrowing(libp2pFuts)

waitFor(main())
