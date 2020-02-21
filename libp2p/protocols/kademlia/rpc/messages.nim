import options

# XXX: Not clear what we want this to look like
# https://github.com/libp2p/specs/tree/master/pubsub/gossipsub#protobuf
# https://github.com/libp2p/specs/blob/master/pubsub/README.md

# http://xlattice.sourceforge.net/components/protocol/kademlia/specs.html
# > All RPC packets are required to carry an RPC identifier assigned by the sender and echoed in the reply. This is a quasi-random number of length B (160 bits).

# ping() -> pong()
# findnode(id) -> return k tcontacts

# https://github.com/libp2p/specs/pull/108/files err

# store
# findvalue

# XXX: Right now this is modelled after pubsub, should fix
# Let's try more minimal

# XXX: Use sum type for RPCMsg?
# https://github.com/nim-lang/Nim/wiki/Common-Criticisms#sum-types-are-weird

# TODO: Probably want to use enums here
# https://github.com/libp2p/rust-libp2p/blob/master/protocols/kad/src/dht.proto

# Currently using minimal version of kad-dht without enums

type
  Peer* = object
    id*: seq[byte]
    addrs*: seq[seq[byte]]

  CloserPeers* = object
    peers*: seq[Peer]

  MessageType* = object
    # TODO: replace me with an enum MessageType
    strtype*: string # "PING" "FIND_NODE"

  Key* = object
    id*: seq[byte]

  RPCMsg* = object
    messageType*: MessageType
    key*: Key
    closerPeers*: Option[CloserPeers]

    # Returns values
    #record*: Option[Record]

    # Return peers closer a key in a query
    # E.g. FIND_NODE
