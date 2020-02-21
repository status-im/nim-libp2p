import options
import chronicles
import messages,
       ../../../protobuf/minprotobuf
import ../../../peer
 
# XXX: This manual decoding is kind of meh

proc decodePeers*(pb: var ProtoBuffer): seq[Peer] {.gcsafe.} =
  debug "decoding peers msg"

  # XXX: Is this wrong level? peer vs seq[peer]
  while true:
    var peer: Peer
    # TODO: HERE ATM. Fix field issue. FIELD 2
#    if pb.enterSubMessage() > 0:
    if pb.getBytes(1, peer.id) < 0:
      debug "id field missing from closerPeers msg"
      break

    debug "read id field", id = peer.id

    while true:
      # XXX: String?
      var ma: seq[byte]
      if pb.getBytes(2, ma) < 0:
        break
      debug "read addr field", ma = ma
      peer.addrs.add(ma)

    debug "decode peer adding peer", peer = peer
    result.add(peer)

proc decodeCloserPeers*(pb: var ProtoBuffer): Option[CloserPeers] {.gcsafe.} =
  debug "decoding closerpeers msg"
  var closerPeers: CloserPeers
  #var peers: seq[Peer]
  while true:
    # XXX: Alt to sub message here ?
    var field = pb.enterSubMessage()
    debug "processing submessage", field = field
    case field:
    of 0:
      debug "no submessage found in CloserPeers msg"
      break
    of 1:
      debug "HELLO 8-1 OLD FRIEND"
      closerPeers.peers = pb.decodePeers()
    of 2:
      debug "HELLO 8-2 WHO BE U?"
    else:
      raise newException(CatchableError, "message type not recognized")

    if result.isNone:
      result = some(closerPeers)

proc decodeMessageType*(pb: var ProtoBuffer): MessageType {.gcsafe.} =
  debug "decoding message type"
  
  while true:
    var messageType: MessageType
    # if pb.enterSubMessage() > 0:
    if pb.getString(1, messageType.strtype) < 0:
      debug "unable to find field 1, message type"
      break
    debug "read message type field", strtype = messageType.strtype

    return messageType

proc decodeKey*(pb: var ProtoBuffer): Key {.gcsafe.} =
  debug "decoding key"

  while true:
    var key: Key
    #if pb.enterSubMessage() > 0:
    if pb.getBytes(1, key.id) < 0:
      echo "unable to find field 1 key id"
      break
    debug "read type field", result = result

    return key

proc encodePeer(peer: Peer, pb: var ProtoBuffer) {.gcsafe.} =
  pb.write(initProtoField(1, peer.id))
  for ma in peer.addrs:
    debug "encodePeer ma", ma = ma
    # TODO: Check seq[byte], should work
    pb.write(initProtoField(2, ma))
    
proc encodeCloserPeers(closerPeers: CloserPeers, pb: var ProtoBuffer) {.gcsafe.} =
  if closerPeers.peers.len > 0:
    var peers = initProtoBuffer()
    for p in closerPeers.peers:
      debug "encoding closer peer", p = p
      p.encodePeer(peers)

    # write peers to protobuf
    peers.finish()
    pb.write(initProtoField(1, peers))

proc encodeMessageType(mt: MessageType, pb: var ProtoBuffer) {.gcsafe.} =
  debug "encodeMessageType"
  pb.write(initProtofield(1, mt.strtype))

proc encodeKey(key: Key, pb: var ProtoBuffer) {.gcsafe.} =
  debug "encodeKey"
  pb.write(initProtofield(1, key.id))

proc encodeRpcMsg*(msg: RPCMsg): ProtoBuffer {.gcsafe.} =
  result = initProtoBuffer()
  trace "encoding msg: ", msg = msg, strtype = msg.strtype

    #  while true:
    # XXX: I would expect to just work, but it doesn't

  var messageType = initProtoBuffer()
  # write here
  # encodeMessage(msg.messageType, messageType)
  encodeMessageType(msg.messageType, messageType)
  messageType.finish()
  result.write(initProtoField(1, messageType))
  # XXX: Without this it doesn't seem to detect field
  #result.finish()

  var key = initProtoBuffer()
  encodeKey(msg.key, key)
  key.finish()
  result.write(initProtoField(2, key))
  #result.finish()

  if msg.closerPeers.isSome:
    var closerPeers = initProtoBuffer()

    msg.closerPeers.get.encodeCloserPeers(closerPeers)
    
    # write closerPeers to protobuf
    closerPeers.finish()
    result.write(initProtoField(8, closerPeers))

  if result.buffer.len > 0:
    result.finish()

proc decodeRpcMsg*(msg: seq[byte]): RPCMsg {.gcsafe.} =
  var pb = initProtoBuffer(msg)

  while true:
    var field  = pb.enterSubMessage()
    debug "processing submessage", field = field
    case field:
    # XXX: What is field 0?
    of 0:
      debug "no submessage found in RPC msg"
      break
    of 1:
      # TODO:Deal with
      # XXX: Why is this multipel times?
      debug "HELLO 1 OLD FRIEND"
      result.messageType = pb.decodeMessageType()
      #msg.closerPeers = pb.decodeCloserPeers()
      #if pb.getString(1, msg.strtype) < 0:
      #  debug "strtype field missing from msg" 
      #  break
      #debug "read message field msg.strtype", strtype = msg.strtype
      #break
    of 2:
      debug "HELLO 2 OLD FRIEND"
      result.key = pb.decodeKey()
    of 8:
      # XXX: never hit
      debug "HELLO 8 OLD FRIEND"
      result.closerPeers = pb.decodeCloserPeers()
    else:
      raise newException(CatchableError, "*** message type not recognized")
