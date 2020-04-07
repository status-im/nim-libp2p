## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

when not(compileOption("threads")):
  {.fatal: "Please, compile this program with the --threads:on option!".}

import tables, options, sequtils, algorithm, strformat, os, strutils
import chronos
import ../libp2p/libp2p

const ChatCodec = "/nim-libp2p/chat/1.0.0"
const DefaultAddr = "/ip4/127.0.0.1/tcp/55505"

const Help = """
  Commands: /[?|hep|connect|disconnect|exit]
  help: Prints this help
  connect: dials a remote peer
  disconnect: ends current session
  exit: closes the chat
"""

type
  CustomData = ref object
    consoleFd: AsyncFD
    serveFut: Future[void]

  ChatProto = ref object of LPProtocol
    customData*: CustomData
    switch: Switch
    transp: StreamTransport
    conn: Connection
    client: bool
    connected: bool
    started: bool

proc id (p: ChatProto): string =
  if not isNil(p.conn.peerInfo):
    $p.conn.peerInfo.peerId
  else:
    "unknown"

# forward declaration
proc readWriteLoop(p: ChatProto) {.async, gcsafe.}
proc readAndPrint(p: ChatProto) {.async, gcsafe.} =
  while true:
    while p.connected:
      # echo &"{p.id} -> "
      echo cast[string](await p.conn.readLp())
    await sleepAsync(100.millis)

proc dialPeer(p: ChatProto, address: string) {.async, gcsafe.} =
  var parts = address.split("/")
  if parts.len == 11 and parts[^2] notin ["ipfs", "p2p"]:
    quit("invalid or incompelete peerId")

  var remotePeer = PeerInfo.init(parts[^1],
                                 [MultiAddress.init(address)])

  echo &"dialing peer: {address}"
  p.conn = await p.switch.dial(remotePeer, ChatCodec)
  p.connected = true

proc writeAndPrint(p: ChatProto) {.async, gcsafe.} =
  while true:
    if not p.connected:
      # echo &"{p.id} ->"
    # else:
      echo "type an address or wait for a connection:"
      echo "type /[help|?] for help"

    var line = await p.transp.readLine()
    if line.startsWith("/help") or line.startsWith("/?") or not p.started:
      echo Help
      continue

    if line.startsWith("/disconnect"):
      echo "Ending current session"
      if p.connected and p.conn.closed.not:
        await p.conn.close()
      p.connected = false
    elif line.startsWith("/connect"):
      if p.connected:
        var yesno = "N"
        echo "a session is already in progress, do you want end it [y/N]?"
        yesno = await p.transp.readLine()
        if yesno.cmpIgnoreCase("y") == 0:
          await p.conn.close()
          p.connected = false
        elif yesno.cmpIgnoreCase("n") == 0:
          continue
        else:
          echo "unrecognized response"
          continue

      echo "enter address of remote peer"
      let address = await p.transp.readLine()
      if address.len > 0:
        await p.dialPeer(address)

    elif line.startsWith("/exit"):
      if p.connected and p.conn.closed.not:
        await p.conn.close()
        p.connected = false

      await p.switch.stop()
      echo "quitting..."
      quit(0)
    else:
      if p.connected:
        await p.conn.writeLp(line)
      else:
        try:
          if line.startsWith("/") and "ipfs" in line:
            await p.dialPeer(line)
        except:
          echo &"unable to dial remote peer {line}"
          # echo getCurrentExceptionMsg()

proc readWriteLoop(p: ChatProto) {.async, gcsafe.} =
  asyncCheck p.writeAndPrint()
  asyncCheck p.readAndPrint()

method init(p: ChatProto) {.gcsafe.} =
  proc handle(stream: Connection, proto: string) {.async, gcsafe.} =
    if p.connected and not p.conn.closed:
      echo "a chat session is already in progress - disconnecting!"
      await stream.close()
    else:
      p.conn = stream
      p.connected = true

  p.codec = ChatCodec
  p.handler = handle

proc newChatProto(switch: Switch, transp: StreamTransport): ChatProto =
  new result
  result.switch = switch
  result.transp = transp
  result.init()

proc threadMain(wfd: AsyncFD) {.thread.} =
  ## This procedure performs reading from `stdin` and sends data over
  ## pipe to main thread.
  var transp = fromPipe(wfd)

  while true:
    var line = stdin.readLine()
    discard waitFor transp.write(line & "\r\n")

proc serveThread(customData: CustomData) {.async.} =
  var transp = fromPipe(customData.consoleFd)

  let seckey = PrivateKey.random(RSA)
  var peerInfo = PeerInfo.init(seckey)
  var localAddress = DefaultAddr
  while true:
    echo &"Type an address to bind to or Enter to use the default {DefaultAddr}"
    let a = await transp.readLine()
    try:
      if a.len > 0:
        peerInfo.addrs.add(Multiaddress.init(a))
        break

      peerInfo.addrs.add(Multiaddress.init(localAddress))
      break
    except:
      echo "invalid address"
      localAddress = DefaultAddr
      continue

  proc createMplex(conn: Connection): Muxer =
    result = newMplex(conn)

  var mplexProvider = newMuxerProvider(createMplex, MplexCodec)
  var transports = @[Transport(newTransport(TcpTransport))]
  var muxers = [(MplexCodec, mplexProvider)].toTable()
  var identify = newIdentify(peerInfo)
  var secureManagers = [(SecioCodec, Secure(newSecio(seckey)))].toTable()
  var switch = newSwitch(peerInfo,
                         transports,
                         identify,
                         muxers,
                         secureManagers = secureManagers)

  var chatProto = newChatProto(switch, transp)
  switch.mount(chatProto)
  var libp2pFuts = await switch.start()
  chatProto.started = true

  let id = peerInfo.peerId.pretty
  echo "PeerID: " & id
  echo "listening on: "
  for a in peerInfo.addrs:
    echo &"{a}/ipfs/{id}"

  await chatProto.readWriteLoop()
  await allFutures(libp2pFuts)

proc main() {.async.} =
  var data = new CustomData

  var (rfd, wfd) = createAsyncPipe()
  if rfd == asyncInvalidPipe or wfd == asyncInvalidPipe:
    raise newException(ValueError, "Could not initialize pipe!")

  data.consoleFd = rfd
  data.serveFut = serveThread(data)
  var thread: Thread[AsyncFD]
  thread.createThread(threadMain, wfd)
  await data.serveFut

when isMainModule:
  waitFor(main())
