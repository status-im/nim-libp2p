import options, hashes, strutils, tables, hashes
import chronos, chronicles
import rpc/[messages, protobuf]
import ../../peer,
       ../../peerinfo,
       ../../connection,
       ../../stream/lpstream,
       ../../crypto/crypto,
       ../../protobuf/minprotobuf

logScope:
  topic = "KadPeer"

type
    KadPeer* = ref object of RootObj
      proto: string # the protocol that this peer joined from
      sendConn: Connection
      peerInfo*: PeerInfo
      handler*: RPCHandler
      refs*: int # refcount of the connections this peer is handling
      onConnect: AsyncEvent

    RPCHandler* = proc(peer: KadPeer, msgs: seq[RPCMsg]): Future[void] {.gcsafe.}

proc id*(p: KadPeer): string = p.peerInfo.id

proc isConnected*(p: KadPeer): bool =
  (not isNil(p.sendConn))

proc `conn=`*(p: KadPeer, conn: Connection) =
  trace "attaching send connection for peer", peer = p.id
  p.sendConn = conn
  p.onConnect.fire()


# TODO: Add caching of messages
proc handle*(p: KadPeer, conn: Connection) {.async.} =
  trace "handling kademlia rpc", peer = p.id, closed = conn.closed
  try:
    while not conn.closed:
      trace "waiting for data", peer = p.id, closed = conn.closed
      let data = await conn.readLp()
      let hexData = data.toHex()
      trace "read data from peer", peer = p.id, data = hexData

      let msg = decodeRpcMsg(data)
      debug "decoded msg from peer", peer = p.id, msg = msg
      await p.handler(p, @[msg])
  except CatchableError as exc:
    error "exception occured", exc = exc.msg
  finally:
    trace "exiting kad peer read loop", peer = p.id
    if not conn.closed():
      await conn.close()

# Waits for response, only sends one message
proc sendWait*(p: KadPeer, msgs: seq[RPCMsg]): Future[seq[RPCMsg]] {.async.} =
  try:
    for m in msgs:
      debug "sending msgs to peer", toPeer = p.id
      let encoded = encodeRpcMsg(m)
      let encodedHex = encoded.buffer.toHex()
      if encoded.buffer.len <= 0:
        debug "empty message, skipping", peer = p.id
        return

      # TODO: Implement caching
 
      proc sendToRemote {.async.} =
        debug "sending encoded msgs to peer", peer = p.id, encoded = encodedHex
        await p.sendConn.writeLp(encoded.buffer)

      if p.isConnected:
        await sendToRemote()
        debug "*** Waiting for remote to respond"
        # XXX: Here atm, not hit
        var res = cast[string](await p.sendConn.readLp())
        debug "*** Remote responded", res = res
        return

      # Get response how?

      # TODO: handle queuing iff no connection for later delivery

  except CatchableError as exc:
    trace "exception occured", exc = exc.msg


proc send*(p: KadPeer, msgs: seq[RPCMsg]) {.async.} =
  try:
    for m in msgs:
      debug "sending msgs to peer", toPeer = p.id
      let encoded = encodeRpcMsg(m)
      let encodedHex = encoded.buffer.toHex()
      if encoded.buffer.len <= 0:
        debug "empty message, skipping", peer = p.id
        return

      # TODO: Implement caching
 
      proc sendToRemote {.async.} =
        debug "sending encoded msgs to peer", peer = p.id, encoded = encodedHex
        await p.sendConn.writeLp(encoded.buffer)

      if p.isConnected:
        await sendToRemote()
        return

      # TODO: handle queuing iff no connection for later delivery

  except CatchableError as exc:
    trace "exception occured", exc = exc.msg


proc newKadPeer*(peerInfo: PeerInfo,
                 proto: string): KadPeer =
  new result
  result.proto = proto
  result.peerInfo = peerInfo
  result.onConnect = newAsyncEvent()
