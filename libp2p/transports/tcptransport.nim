## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos, chronicles, sequtils
import transport,
       ../wire,
       ../connection,
       ../multiaddress,
       ../multicodec,
       ../stream/chronosstream

logScope:
  topic = "TcpTransport"

type TcpTransport* = ref object of Transport
  server*: StreamServer

proc connHandler*(t: Transport,
                  server: StreamServer,
                  client: StreamTransport,
                  initiator: bool = false):
                  Future[Connection] {.async, gcsafe.} =
  trace "handling connection for", address = $client.remoteAddress
  let conn: Connection = newConnection(newChronosStream(server, client))
  conn.observedAddrs = MultiAddress.init(client.remoteAddress)
  if not initiator:
    let handlerFut = if isNil(t.handler): nil else: t.handler(conn)
    let connHolder: ConnHolder = ConnHolder(connection: conn,
                                            connFuture: handlerFut)
    # TODO: this needs rethinking,
    # currently it leaks since there
    # is no way to delete the conn on close
    # t.connections.add(connHolder)
  result = conn

proc connCb(server: StreamServer,
            client: StreamTransport) {.async, gcsafe.} =
  trace "incomming connection for", address = $client.remoteAddress
  let t: Transport = cast[Transport](server.udata)
  asyncCheck t.connHandler(server, client)

method init*(t: TcpTransport) =
  t.multicodec = multiCodec("tcp")

method close*(t: TcpTransport): Future[void] {.async, gcsafe.} =
  ## start the transport
  trace "stopping transport"
  await procCall Transport(t).close() # call base

  # server can be nil
  if t.server != nil:
    t.server.stop()
    t.server.close()
  trace "transport stopped"

method listen*(t: TcpTransport,
               ma: MultiAddress,
               handler: ConnHandler):
               Future[Future[void]] {.async, gcsafe.} =
  discard await procCall Transport(t).listen(ma, handler) # call base

  ## listen on the transport
  t.server = createStreamServer(t.ma, connCb, {}, t)
  t.server.start()

  # always get the resolved address in case we're bound to 0.0.0.0:0
  t.ma = MultiAddress.init(t.server.sock.getLocalAddress())
  result = t.server.join()
  trace "started node on", address = t.ma

method dial*(t: TcpTransport,
             address: MultiAddress):
             Future[Connection] {.async, gcsafe.} =
  trace "dialing remote peer", address = $address
  ## dial a peer
  let client: StreamTransport = await connect(address)
  result = await t.connHandler(t.server, client, true)

method handles*(t: TcpTransport, address: MultiAddress): bool {.gcsafe.} =
  if procCall Transport(t).handles(address):
    result = address.protocols.filterIt( it == multiCodec("tcp") ).len > 0
