## Nim-LibP2P
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

## This module implementes API for `go-libp2p-daemon`.
import os, osproc, strutils, tables, strtabs
import chronos, protobuf_serialization
import ../varint, ../multiaddress, ../multicodec, ../cid, ../peerid
import ../wire, ../multihash
import ../crypto/crypto

export peerid, multiaddress, multicodec, multihash, cid, crypto, wire

when not defined(windows):
  import posix

const
  MaxMessageSize = 1'u shl 22
  DefaultSocketPath* = "/unix/tmp/p2pd.sock"
  DefaultUnixSocketPattern* = "/unix/tmp/nim-p2pd-$1-$2.sock"
  DefaultIpSocketPattern* = "/ip4/127.0.0.1/tcp/$2"
  DefaultUnixChildPattern* = "/unix/tmp/nim-p2pd-handle-$1-$2.sock"
  DefaultIpChildPattern* = "/ip4/127.0.0.1/tcp/$2"
  DefaultDaemonFile* = "p2pd"

type
  IpfsLogLevel* {.pure.} = enum
    Critical, Error, Warning, Notice, Info, Debug, Trace

  RequestType* {.pure.} = enum
    IDENTITY = 0,
    CONNECT = 1,
    STREAM_OPEN = 2,
    STREAM_HANDLER = 3,
    DHT = 4,
    LIST_PEERS = 5,
    CONNMANAGER = 6,
    DISCONNECT = 7
    PUBSUB = 8

  DHTRequestType* {.pure.} = enum
    FIND_PEER = 0,
    FIND_PEERS_CONNECTED_TO_PEER = 1,
    FIND_PROVIDERS = 2,
    GET_CLOSEST_PEERS = 3,
    GET_PUBLIC_KEY = 4,
    GET_VALUE = 5,
    SEARCH_VALUE = 6,
    PUT_VALUE = 7,
    PROVIDE = 8

  ConnManagerRequestType* {.pure.} = enum
    TAG_PEER = 0,
    UNTAG_PEER = 1,
    TRIM = 2

  PSRequestType* {.pure.} = enum
    GET_TOPICS = 0,
    LIST_PEERS = 1,
    PUBLISH = 2,
    SUBSCRIBE = 3

  ResponseKind* = enum
    SUCCESS = 0,
    ERROR = 1

  DHTResponseType* {.pure.} = enum
    BEGIN = 0,
    VALUE = 1,
    END = 2

  MultiProtocol* = string
  DHTValue* = seq[byte]

  P2PStreamFlags* {.pure.} = enum
    None, Closed, Inbound, Outbound

  P2PDaemonFlags* = enum
    DHTClient,     ## Start daemon in DHT client mode
    DHTFull,       ## Start daemon with full DHT support
    Bootstrap,     ## Start daemon with bootstrap
    WaitBootstrap, ## Start daemon with bootstrap and wait until daemon
                   ## establish connection to at least 2 peers
    PSFloodSub,    ## Enable `FloodSub` protocol in daemon
    PSGossipSub,   ## Enable `GossipSub` protocol in daemon
    PSNoSign,      ## Disable pubsub message signing (default true)
    PSStrictSign,  ## Force strict checking pubsub message signature
    NATPortMap,    ## Force daemon to use NAT-PMP.
    AutoNAT,       ## Force daemon to use AutoNAT.
    AutoRelay,     ## Enables autorelay mode.
    RelayActive,   ## Enables active mode for relay.
    RelayDiscovery,## Enables passive discovery for relay.
    RelayHop,      ## Enables hop for relay.
    NoInlinePeerID,## Disable inlining of peer ID (not yet in #master).
    NoProcessCtrl  ## Process was not spawned.

  P2PStream* = ref object
    flags*: set[P2PStreamFlags]
    peer*: PeerID
    raddress*: MultiAddress
    protocol*: string
    transp*: StreamTransport

  P2PServer = object
    server*: StreamServer
    address*: MultiAddress

  DaemonAPI* = ref object
    # pool*: TransportPool
    flags*: set[P2PDaemonFlags]
    address*: MultiAddress
    pattern*: string
    ucounter*: int
    process*: Process
    handlers*: Table[string, P2PStreamCallback]
    servers*: seq[P2PServer]
    userData*: RootRef

  PeerInfo* = object
    peer*: PeerID
    addresses*: seq[MultiAddress]

  SerializablePeerInfo {.protobuf2.} = object
    id {.fieldNumber: 1.}: Option[PeerID]
    addresses {.fieldNumber: 2.}: seq[string]

  PubsubTicket* = ref object
    topic*: string
    handler*: P2PPubSubCallback
    transp*: StreamTransport

  PubSubMessage* = object
    peer*: PeerID
    data*: seq[byte]
    seqno*: seq[byte]
    topics*: seq[string]
    signature*: Signature
    key*: PublicKey

  P2PStreamCallback* = proc(api: DaemonAPI,
                            stream: P2PStream): Future[void] {.gcsafe.}
  P2PPubSubCallback* = proc(api: DaemonAPI,
                            ticket: PubsubTicket,
                            message: PubSubMessage): Future[bool] {.gcsafe.}

  DaemonRemoteError* = object of CatchableError
  DaemonLocalError* = object of CatchableError

  SerializableProperties {.protobuf2.} = object
    peerOrAddress {.fieldNumber: 1.}: seq[byte]
    protocolsOrAddresses {.fieldNumber: 2.}: seq[seq[byte]]
    timeout {.fieldNumber: 3, pint.}: PBOption[0'i32]

  SerializableDHTProperties {.protobuf2.} = object
    reqType {.fieldNumber: 1, required, pint.}: DHTRequestType
    peer {.fieldNumber: 2.}: seq[byte]
    cid {.fieldNumber: 3.}: seq[byte]
    key {.fieldNumber: 4.}: PBOption[""]
    value {.fieldNumber: 5.}: seq[byte]
    count {.fieldNumber: 6, pint, required.}: int32
    timeout {.fieldNumber: 7, pint.}: PBOption[0'i32]

  SerializableCMProperties {.protobuf2.} = object
    reqType {.fieldNumber: 1, required, pint.}: ConnManagerRequestType
    peer {.fieldNumber: 2.}: seq[byte]
    tag {.fieldNumber: 3.}: PBOption[""]
    weight {.fieldNumber: 4, pint.}: PBOption[0'i64]

  SerializablePSProperties {.protobuf2.} = object
    reqType {.fieldNumber: 1, required, pint.}: PSRequestType
    topic {.fieldNumber: 2.}: PBOption[""]
    data {.fieldNumber: 3.}: seq[byte]

  SerializableRequest {.protobuf2.} = object
    reqType {.fieldNumber: 1, required, pint.}: RequestType
    connectProperties {.fieldNumber: 2.}: Option[SerializableProperties]
    streamOpenProperties {.fieldNumber: 3.}: Option[SerializableProperties]
    streamHandleProperties {.fieldNumber: 4.}: Option[SerializableProperties]
    dhtProperties {.fieldNumber: 5.}: Option[SerializableDHTProperties]
    cmProperties {.fieldNumber: 6.}: Option[SerializableCMProperties]
    disconnectProperties {.fieldNumber: 7.}: Option[SerializableProperties]
    psProperties {.fieldNumber: 8.}: Option[SerializablePSProperties]

  SerializableError {.protobuf2.} = object
    msg {.fieldNumber: 1.}: PBOption[""]

  SerializableStreamResponse {.protobuf2.} = object
    peer {.fieldNumber: 1, required.}: seq[byte]
    address {.fieldNumber: 2, required.}: seq[byte]
    proto {.fieldNumber: 3, required.}: string

  SerializableDHTResponse {.protobuf2.} = object
    kind {.fieldNumber: 1, pint, required.}: DHTResponseType
    peer {.fieldNumber: 2.}: Option[SerializableProperties]
    value {.fieldNumber: 3.}: seq[byte]

  SerializableTopicsResponse {.protobuf2.} = object
    topics {.fieldNumber: 1.}: seq[string]
    peerIDs {.fieldNumber: 2.}: seq[seq[byte]]

  SerializablePubSubResponse {.protobuf2.} = object
    peer {.fieldNumber: 1.}: seq[byte]
    data {.fieldNumber: 2.}: seq[byte]
    seqno {.fieldNumber: 3.}: seq[byte]
    topics {.fieldNumber: 4.}: seq[string]
    signature {.fieldNumber: 5.}: seq[byte]
    key {.fieldNumber: 6.}: seq[byte]

  SerializableResponse {.protobuf2.} = object
    kind {.fieldNumber: 1, required, pint.}: ResponseKind
    error {.fieldNumber: 2.}: Option[SerializableError]
    streamInfo {.fieldNumber: 3.}: Option[SerializableStreamResponse]
    identify {.fieldNumber: 4.}: Option[SerializableProperties]
    dht {.fieldNumber: 5.}: Option[SerializableDHTResponse]
    peers {.fieldNumber: 6.}: seq[SerializableProperties]
    ps {.fieldNumber: 7.}: Option[SerializableTopicsResponse]

var daemonsCount {.threadvar.}: int

proc requestIdentity(): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doIdentify(req *pb.Request)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.IDENTITY
  ), {VarIntLengthPrefix})

proc requestConnect(peerid: PeerID,
                    addresses: openarray[MultiAddress],
                    timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doConnect(req *pb.Request)`.
  var byteAddresses: seq[seq[byte]] = newSeq[seq[byte]](addresses.len)
  for a in 0 ..< addresses.len:
    byteAddresses[a] = addresses[a].data.buffer

  Protobuf.encode(SerializableRequest(
    reqType: RequestType.CONNECT,
    connectProperties: some(SerializableProperties(
      peerOrAddress: peerid.data,
      protocolsOrAddresses: byteAddresses,
      timeout: pbSome(type(SerializableProperties.timeout),
                       if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDisconnect(peerid: PeerID): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doDisconnect(req *pb.Request)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DISCONNECT,
    disconnectProperties: some(SerializableProperties(
      peerOrAddress: peerid.data,
    ))
  ), {VarIntLengthPrefix})

proc requestStreamOpen(peerid: PeerID,
                       protocols: openarray[string],
                       timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doStreamOpen(req *pb.Request)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.STREAM_OPEN,
    streamOpenProperties: some(SerializableProperties(
      peerOrAddress: peerid.data,
      protocolsOrAddresses: cast[seq[seq[byte]]](@protocols),
      timeout: pbSome(type(SerializableProperties.timeout),
                       if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestStreamHandler(address: MultiAddress,
                          protocols: openarray[MultiProtocol]): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doStreamHandler(req *pb.Request)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.STREAM_HANDLER,
    streamHandleProperties: some(SerializableProperties(
      peerOrAddress: address.data.buffer,
      protocolsOrAddresses: cast[seq[seq[byte]]](@protocols)
    ))
  ), {VarIntLengthPrefix})

proc requestListPeers(): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doListPeers(req *pb.Request)`
  Protobuf.encode(SerializableRequest(reqType: RequestType.LIST_PEERS), {VarIntLengthPrefix})

proc requestDHTFindPeer(peer: PeerID, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindPeer(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.FIND_PEER,
        peer: peer.data,
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTFindPeersConnectedToPeer(peer: PeerID,
                                        timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindPeersConnectedToPeer(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.FIND_PEERS_CONNECTED_TO_PEER,
        peer: peer.data,
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTFindProviders(cid: Cid,
                             count: uint32, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindProviders(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.FIND_PROVIDERS,
        cid: cid.data.buffer,
        count: int32(count),
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTGetClosestPeers(key: string, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetClosestPeers(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.GET_CLOSEST_PEERS,
        key: pbSome(type(SerializableDHTProperties.key), key),
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTGetPublicKey(peer: PeerID, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetPublicKey(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.GET_PUBLIC_KEY,
        peer: peer.data,
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTGetValue(key: string, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetValue(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.GET_VALUE,
        key: pbSome(type(SerializableDHTProperties.key), key),
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ))

proc requestDHTSearchValue(key: string, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTSearchValue(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.SEARCH_VALUE,
        key: pbSome(type(SerializableDHTProperties.key), key),
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTPutValue(key: string, value: openarray[byte],
                        timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTPutValue(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.PUT_VALUE,
        key: pbSome(type(SerializableDHTProperties.key), key),
        value: @value,
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestDHTProvide(cid: Cid, timeout = 0): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTProvide(req *pb.DHTRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.DHT,
    dhtProperties: some(SerializableDHTProperties(
        reqType: DHTRequestType.PROVIDE,
        cid: cid.data.buffer,
        timeout: pbSome(type(SerializableDHTProperties.timeout),
                         if timeout > 0: int32(timeout) else: int32(0))
    ))
  ), {VarIntLengthPrefix})

proc requestCMTagPeer(peer: PeerID, tag: string, weight: int): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L18
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.CONNMANAGER,
    cmProperties: some(SerializableCMProperties(
      reqType: ConnManagerRequestType.TAG_PEER,
      peer: peer.data,
      tag: pbSome(type(SerializableCMProperties.tag), tag),
      weight: pbSome(type(SerializableCMProperties.weight), int64(weight))
    ))
  ))

proc requestCMUntagPeer(peer: PeerID, tag: string): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L33
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.CONNMANAGER,
    cmProperties: some(SerializableCMProperties(
      reqType: ConnManagerRequestType.UNTAG_PEER,
      peer: peer.data,
      tag: pbSome(type(SerializableCMProperties.tag), tag)
    ))
  ), {VarIntLengthPrefix})

proc requestCMTrim(): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L47
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.CONNMANAGER,
    cmProperties: some(SerializableCMProperties(
      reqType: ConnManagerRequestType.TRIM
    ))
  ), {VarIntLengthPrefix})

proc requestPSGetTopics(): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubGetTopics(req *pb.PSRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.PUBSUB,
    psProperties: some(SerializablePSProperties(
      reqType: PSRequestType.GET_TOPICS
    ))
  ), {VarIntLengthPrefix})

proc requestPSListPeers(topic: string): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubListPeers(req *pb.PSRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.PUBSUB,
    psProperties: some(SerializablePSProperties(
      reqType: PSRequestType.LIST_PEERS,
      topic: pbSome(type(SerializablePSProperties.topic), topic)
    ))
  ), {VarIntLengthPrefix})

proc requestPSPublish(topic: string, data: openarray[byte]): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubPublish(req *pb.PSRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.PUBSUB,
    psProperties: some(SerializablePSProperties(
      reqType: PSRequestType.PUBLISH,
      topic: pbSome(type(SerializablePSProperties.topic), topic),
      data: @data
    ))
  ), {VarIntLengthPrefix})

proc requestPSSubscribe(topic: string): seq[byte] =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubSubscribe(req *pb.PSRequest)`.
  Protobuf.encode(SerializableRequest(
    reqType: RequestType.PUBSUB,
    psProperties: some(SerializablePSProperties(
      reqType: PSRequestType.SUBSCRIBE,
      topic: pbSome(type(SerializablePSProperties.topic), topic)
    ))
  ), {VarIntLengthPrefix})

proc recvMessage(conn: StreamTransport): Future[seq[byte]] {.async.} =
  var
    size: uint
    length: int
    res: VarintResult[void]
  var buffer = newSeq[byte](10)
  try:
    for i in 0..<len(buffer):
      await conn.readExactly(addr buffer[i], 1)
      res = PB.getUVarint(buffer.toOpenArray(0, i), length, size)
      if res.isOk():
        break
    if res.isErr() or size > MaxMessageSize:
      buffer.setLen(0)
      result = buffer
      return
    buffer.setLen(size)
    await conn.readExactly(addr buffer[0], int(size))
  except TransportIncompleteError:
    buffer.setLen(0)

  result = buffer

proc newConnection*(api: DaemonAPI): Future[StreamTransport] =
  result = connect(api.address)

proc closeConnection*(api: DaemonAPI, transp: StreamTransport): Future[void] =
  result = transp.closeWait()

proc socketExists(address: MultiAddress): Future[bool] {.async.} =
  try:
    var transp = await connect(address)
    await transp.closeWait()
    result = true
  except:
    result = false

when defined(windows):
  proc getCurrentProcessId(): uint32 {.stdcall, dynlib: "kernel32",
                                       importc: "GetCurrentProcessId".}
  proc getProcessId(): int =
    result = cast[int](getCurrentProcessId())
else:
  proc getProcessId(): int =
    result = cast[int](posix.getpid())

proc getSocket(pattern: string,
               count: ptr int): Future[MultiAddress] {.async.} =
  var sockname = ""
  var pid = $getProcessId()
  sockname = pattern % [pid, $(count[])]
  let tmpma = MultiAddress.init(sockname).tryGet()

  if UNIX.match(tmpma):
    while true:
      count[] = count[] + 1
      sockname = pattern % [pid, $(count[])]
      var ma = MultiAddress.init(sockname).tryGet()
      let res = await socketExists(ma)
      if not res:
        result = ma
        break
  elif TCP.match(tmpma):
    sockname = pattern % [pid, "0"]
    var ma = MultiAddress.init(sockname).tryGet()
    var sock = createAsyncSocket(ma)
    if sock.bindAsyncSocket(ma):
      # Socket was successfully bound, then its free to use
      count[] = count[] + 1
      var ta = sock.getLocalAddress()
      sockname = pattern % [pid, $ta.port]
      result = MultiAddress.init(sockname).tryGet()
    closeSocket(sock)

# This is forward declaration needed for newDaemonApi()
proc listPeers*(api: DaemonAPI): Future[seq[PeerInfo]] {.async, gcsafe.}

proc copyEnv(): StringTableRef =
  ## This procedure copy all environment variables into StringTable.
  result = newStringTable(modeStyleInsensitive)
  for key, val in envPairs():
    result[key] = val

proc newDaemonApi*(flags: set[P2PDaemonFlags] = {},
                   bootstrapNodes: seq[string] = @[],
                   id: string = "",
                   hostAddresses: seq[MultiAddress] = @[],
                   announcedAddresses: seq[MultiAddress] = @[],
                   daemon = DefaultDaemonFile,
                   sockpath = "",
                   patternSock = "",
                   patternHandler = "",
                   poolSize = 10,
                   gossipsubHeartbeatInterval = 0,
                   gossipsubHeartbeatDelay = 0,
                   peersRequired = 2,
                   logFile = "",
                   logLevel = IpfsLogLevel.Debug): Future[DaemonAPI] {.async.} =
  ## Initialize connection to `go-libp2p-daemon` control socket.
  ##
  ## ``flags`` - set of P2PDaemonFlags.
  ##
  ## ``bootstrapNodes`` - list of bootnode's addresses in MultiAddress format.
  ## (default: @[], which means usage of default nodes inside of
  ## `go-libp2p-daemon`).
  ##
  ## ``id`` - path to file with identification information (default: "" which
  ## means - generate new random identity).
  ##
  ## ``hostAddresses`` - list of multiaddrs the host should listen on.
  ## (default: @[], the daemon will pick a listening port at random).
  ##
  ## ``announcedAddresses`` - list of multiaddrs the host should announce to
  ##  the network (default: @[], the daemon will announce its own listening
  ##  address).
  ##
  ## ``daemon`` - name of ``go-libp2p-daemon`` executable (default: "p2pd").
  ##
  ## ``sockpath`` - default control socket MultiAddress
  ## (default: "/unix/tmp/p2pd.sock").
  ##
  ## ``patternSock`` - MultiAddress pattern string, used to start multiple
  ## daemons (default on Unix: "/unix/tmp/nim-p2pd-$1-$2.sock", on Windows:
  ## "/ip4/127.0.0.1/tcp/$2").
  ##
  ## ``patternHandler`` - MultiAddress pattern string, used to establish
  ## incoming channels (default on Unix: "/unix/tmp/nim-p2pd-handle-$1-$2.sock",
  ## on Windows: "/ip4/127.0.0.1/tcp/$2").
  ##
  ## ``poolSize`` - size of connections pool (default: 10).
  ##
  ## ``gossipsubHeartbeatInterval`` - GossipSub protocol heartbeat interval in
  ## milliseconds (default: 0, use default `go-libp2p-daemon` values).
  ##
  ## ``gossipsubHeartbeatDelay`` - GossipSub protocol heartbeat delay in
  ## millseconds (default: 0, use default `go-libp2p-daemon` values).
  ##
  ## ``peersRequired`` - Wait until `go-libp2p-daemon` will connect to at least
  ## ``peersRequired`` peers before return from `newDaemonApi()` procedure
  ## (default: 2).
  ##
  ## ``logFile`` - Enable ``go-libp2p-daemon`` logging and store it to file
  ## ``logFile`` (default: "", no logging)
  ##
  ## ``logLevel`` - Set ``go-libp2p-daemon`` logging verbosity level to
  ## ``logLevel`` (default: Debug)
  var api = new DaemonAPI
  var args = newSeq[string]()
  var env: StringTableRef

  when defined(windows):
    var patternForSocket = if len(patternSock) > 0:
      patternSock
    else:
      DefaultIpSocketPattern
    var patternForChild = if len(patternHandler) > 0:
      patternHandler
    else:
      DefaultIpChildPattern
  else:
    var patternForSocket = if len(patternSock) > 0:
      patternSock
    else:
      DefaultUnixSocketPattern
    var patternForChild = if len(patternHandler) > 0:
      patternHandler
    else:
      DefaultUnixChildPattern

  api.flags = flags
  api.servers = newSeq[P2PServer]()
  api.pattern = patternForChild
  api.ucounter = 1
  api.handlers = initTable[string, P2PStreamCallback]()

  if len(sockpath) == 0:
    api.flags.excl(NoProcessCtrl)
    api.address = await getSocket(patternForSocket, addr daemonsCount)
  else:
    api.address = MultiAddress.init(sockpath).tryGet()
    api.flags.incl(NoProcessCtrl)
    let res = await socketExists(api.address)
    if not res:
      raise newException(DaemonLocalError, "Could not connect to remote daemon")
    result = api
    return

  # DHTFull and DHTClient could not be present at the same time
  if DHTFull in flags and DHTClient in flags:
    api.flags.excl(DHTClient)
  # PSGossipSub and PSFloodSub could not be present at the same time
  if PSGossipSub in flags and PSFloodSub in flags:
    api.flags.excl(PSFloodSub)
  if DHTFull in api.flags:
    args.add("-dht")
  if DHTClient in api.flags:
    args.add("-dhtClient")
  if {Bootstrap, WaitBootstrap} * api.flags != {}:
    args.add("-b")
  if len(logFile) != 0:
    env = copyEnv()
    env["IPFS_LOGGING_FMT"] = "nocolor"
    env["GOLOG_FILE"] = logFile
    case logLevel
    of IpfsLogLevel.Critical:
      env["IPFS_LOGGING"] = "CRITICAL"
    of IpfsLogLevel.Error:
      env["IPFS_LOGGING"] = "ERROR"
    of IpfsLogLevel.Warning:
      env["IPFS_LOGGING"] = "WARNING"
    of IpfsLogLevel.Notice:
      env["IPFS_LOGGING"] = "NOTICE"
    of IpfsLogLevel.Info:
      env["IPFS_LOGGING"] = "INFO"
    of IpfsLogLevel.Debug:
      env["IPFS_LOGGING"] = "DEBUG"
    of IpfsLogLevel.Trace:
      env["IPFS_LOGGING"] = "DEBUG"
      env["GOLOG_TRACING_FILE"] = logFile
  if PSGossipSub in api.flags:
    args.add("-pubsub")
    args.add("-pubsubRouter=gossipsub")
    if gossipsubHeartbeatInterval != 0:
      let param = $gossipsubHeartbeatInterval & "ms"
      args.add("-gossipsubHeartbeatInterval=" & param)
    if gossipsubHeartbeatDelay != 0:
      let param = $gossipsubHeartbeatDelay & "ms"
      args.add("-gossipsubHeartbeatInitialDelay=" & param)
  if PSFloodSub in api.flags:
    args.add("-pubsub")
    args.add("-pubsubRouter=floodsub")
  if api.flags * {PSFloodSub, PSGossipSub} != {}:
    if PSNoSign in api.flags:
      args.add("-pubsubSign=false")
    if PSStrictSign in api.flags:
      args.add("-pubsubSignStrict=true")
  if NATPortMap in api.flags:
    args.add("-natPortMap=true")
  if AutoNAT in api.flags:
    args.add("-autonat=true")
  if AutoRelay in api.flags:
    args.add("-autoRelay=true")
  if RelayActive in api.flags:
    args.add("-relayActive=true")
  if RelayDiscovery in api.flags:
    args.add("-relayDiscovery=true")
  if RelayHop in api.flags:
    args.add("-relayHop=true")
  if NoInlinePeerID in api.flags:
    args.add("-noInlinePeerID=true")
  if len(bootstrapNodes) > 0:
    args.add("-bootstrapPeers=" & bootstrapNodes.join(","))
  if len(id) != 0:
    args.add("-id=" & id)
  if len(hostAddresses) > 0:
    var opt = "-hostAddrs="
    for i, address in hostAddresses:
      if i > 0: opt.add ","
      opt.add $address
    args.add(opt)
  if len(announcedAddresses) > 0:
    var opt = "-announceAddrs="
    for i, address in announcedAddresses:
      if i > 0: opt.add ","
      opt.add $address
    args.add(opt)
  args.add("-noise=true")
  args.add("-listen=" & $api.address)

  # We are trying to get absolute daemon path.
  let cmd = findExe(daemon)
  if len(cmd) == 0:
    raise newException(DaemonLocalError, "Could not find daemon executable!")

  # Starting daemon process
  # echo "Starting ", cmd, " ", args.join(" ")
  api.process = startProcess(cmd, "", args, env, {poParentStreams})
  # Waiting until daemon will not be bound to control socket.
  while true:
    if not api.process.running():
      raise newException(DaemonLocalError,
                         "Daemon executable could not be started!")
    let res = await socketExists(api.address)
    if res:
      break
    await sleepAsync(500.milliseconds)

  if WaitBootstrap in api.flags:
    while true:
      var peers = await listPeers(api)
      if len(peers) >= peersRequired:
        break
      await sleepAsync(1.seconds)

  result = api

proc close*(stream: P2PStream) {.async.} =
  ## Close ``stream``.
  if P2PStreamFlags.Closed notin stream.flags:
    await stream.transp.closeWait()
    stream.transp = nil
    stream.flags.incl(P2PStreamFlags.Closed)
  else:
    raise newException(DaemonLocalError, "Stream is already closed!")

proc close*(api: DaemonAPI) {.async.} =
  ## Shutdown connections to `go-libp2p-daemon` control socket.
  # await api.pool.close()
  # Closing all pending servers.
  if len(api.servers) > 0:
    var pending = newSeq[Future[void]]()
    for server in api.servers:
      server.server.stop()
      server.server.close()
      pending.add(server.server.join())
    await allFutures(pending)
    for server in api.servers:
      let address = initTAddress(server.address)
      discard tryRemoveFile($address)
    api.servers.setLen(0)
  # Closing daemon's process.
  if NoProcessCtrl notin api.flags:
    when defined(windows):
      api.process.kill()
    else:
      api.process.terminate()
    discard api.process.waitForExit()
  # Attempt to delete unix socket endpoint.
  let address = initTAddress(api.address)
  if address.family == AddressFamily.Unix:
    discard tryRemoveFile($address)

proc transactMessage(transp: StreamTransport,
                     pbArg: seq[byte]): Future[SerializableResponse] {.async.} =
  var pb = pbArg
  let length = pb.len
  let res = await transp.write(addr pb[0], length)
  if res != length:
    raise newException(DaemonLocalError, "Could not send message to daemon!")
  var message = await transp.recvMessage()
  if len(message) == 0:
    raise newException(DaemonLocalError, "Incorrect or empty message received!")
  try:
    result = Protobuf.decode(message, SerializableResponse)
  except ProtobufReadError:
    raise newException(DaemonLocalError, "Incorrect message received!")

proc getPeer(res: SerializableProperties | SerializableStreamResponse): PeerInfo =
  when res is SerializableProperties:
    result = PeerInfo(peer: PeerID(data: res.peerOrAddress))
  else:
    result = PeerInfo(peer: PeerID(data: res.peer))

  when res is SerializableProperties:
    for address in res.protocolsOrAddresses:
      result.addresses.add(MultiAddress.init(address).tryGet())

proc identity*(api: DaemonAPI): Future[PeerInfo] {.async.} =
  ## Get Node identity information
  var transp = await api.newConnection()
  try:
    let res = await transactMessage(transp, requestIdentity())
    if res.identify.isSome():
      result = getPeer(res.identify.get())
  finally:
    await api.closeConnection(transp)

proc connect*(api: DaemonAPI, peer: PeerID,
              addresses: seq[MultiAddress],
              timeout = 0) {.async.} =
  ## Connect to remote peer with id ``peer`` and addresses ``addresses``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestConnect(peer, addresses, timeout))
  except:
    await api.closeConnection(transp)

proc disconnect*(api: DaemonAPI, peer: PeerID) {.async.} =
  ## Disconnect from remote peer with id ``peer``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestDisconnect(peer))
  finally:
    await api.closeConnection(transp)

proc openStream*(api: DaemonAPI, peer: PeerID,
                 protocols: seq[string],
                 timeout = 0): Future[P2PStream] {.async.} =
  ## Open new stream to peer ``peer`` using one of the protocols in
  ## ``protocols``. Returns ``StreamTransport`` for the stream.
  var transp = await api.newConnection()
  result = new P2PStream
  try:
    var pb = await transp.transactMessage(requestStreamOpen(peer, protocols, timeout))

    if pb.streamInfo.isSome():
      result.protocol = ""
      result.peer = pb.streamInfo.get().getPeer().peer
      result.raddress = MultiAddress.init(pb.streamInfo.get().address).tryGet()
      result.protocol = pb.streamInfo.get().proto
      result.flags.incl(Outbound)
      result.transp = transp
  except Exception as exc:
    await api.closeConnection(transp)
    raise exc

proc streamHandler(server: StreamServer, transp: StreamTransport) {.async.} =
  var
    api = getUserData[DaemonAPI](server)
    message = await transp.recvMessage()
    res = Protobuf.decode(message, SerializableStreamResponse)
    stream = new P2PStream
  stream.protocol = res.proto
  stream.peer = res.getPeer().peer
  stream.raddress = MultiAddress.init(res.address).tryGet()
  stream.flags.incl(Inbound)
  stream.transp = transp
  if len(stream.protocol) > 0:
    var handler = api.handlers.getOrDefault(stream.protocol)
    if not isNil(handler):
      asyncCheck handler(api, stream)

proc addHandler*(api: DaemonAPI, protocols: seq[string],
                 handler: P2PStreamCallback) {.async.} =
  ## Add stream handler ``handler`` for set of protocols ``protocols``.
  var transp = await api.newConnection()
  let maddress = await getSocket(api.pattern, addr api.ucounter)
  var server = createStreamServer(maddress, streamHandler, udata = api)
  try:
    for item in protocols:
      api.handlers[item] = handler
    server.start()
    discard await transp.transactMessage(requestStreamHandler(maddress,
                                                               protocols))
    api.servers.add(P2PServer(server: server, address: maddress))
  except Exception as exc:
    for item in protocols:
      api.handlers.del(item)
    server.stop()
    server.close()
    await server.join()
    raise exc
  finally:
    await api.closeConnection(transp)

proc listPeers*(api: DaemonAPI): Future[seq[PeerInfo]] {.async.} =
  ## Get list of remote peers to which we are currently connected.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestListPeers())
    result = newSeq[PeerInfo]()
    for peer in res.peers:
      result.add(peer.getPeer())
  finally:
    await api.closeConnection(transp)

proc cmTagPeer*(api: DaemonAPI, peer: PeerID, tag: string,
              weight: int) {.async.} =
  ## Tag peer with id ``peer`` using ``tag`` and ``weight``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestCMTagPeer(peer, tag, weight))
  finally:
    await api.closeConnection(transp)

proc cmUntagPeer*(api: DaemonAPI, peer: PeerID, tag: string) {.async.} =
  ## Remove tag ``tag`` from peer with id ``peer``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestCMUntagPeer(peer, tag))
  finally:
    await api.closeConnection(transp)

proc cmTrimPeers*(api: DaemonAPI) {.async.} =
  ## Trim all connections.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestCMTrim())
  finally:
    await api.closeConnection(transp)

proc dhtFindPeer*(api: DaemonAPI, peer: PeerID,
                  timeout = 0): Future[PeerInfo] {.async.} =
  ## Find peer with id ``peer`` and return peer information ``PeerInfo``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestDHTFindPeer(peer, timeout))
    if (
      (res.dht.isSome()) and
      (res.dht.get().kind == DHTResponseType.VALUE) and
      (res.dht.get().peer.isSome())
    ):
      result = res.dht.get().peer.get().getPeer()
    else:
      raise newException(DaemonLocalError, "Missing required field or incorrect repsonse type!")
  finally:
    await api.closeConnection(transp)

proc dhtGetPublicKey*(api: DaemonAPI, peer: PeerID,
                      timeout = 0): Future[PublicKey] {.async.} =
  ## Get peer's public key from peer with id ``peer``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestDHTGetPublicKey(peer, timeout))
    if (res.dht.isSome()) and (res.dht.get().kind == DHTResponseType.VALUE):
      if not result.init(res.dht.get().value):
        raise newException(DaemonLocalError, "Missing field `value`!")
    else:
      raise newException(DaemonLocalError, "Missing required field or incorrect repsonse type!")
  finally:
    await api.closeConnection(transp)

proc dhtGetValue*(api: DaemonAPI, key: string,
                  timeout = 0): Future[seq[byte]] {.async.} =
  ## Get value associated with ``key``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestDHTGetValue(key, timeout))
    if (res.dht.isSome()) and (res.dht.get().kind == DHTResponseType.VALUE):
      result = res.dht.get().value
    else:
      raise newException(DaemonLocalError, "Missing required field or incorrect repsonse type!")
  finally:
    await api.closeConnection(transp)

proc dhtPutValue*(api: DaemonAPI, key: string, value: seq[byte],
                  timeout = 0) {.async.} =
  ## Associate ``value`` with ``key``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestDHTPutValue(key, value,
                                                             timeout))
  finally:
    await api.closeConnection(transp)

proc dhtProvide*(api: DaemonAPI, cid: Cid, timeout = 0) {.async.} =
  ## Provide content with id ``cid``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestDHTProvide(cid, timeout))
  finally:
    await api.closeConnection(transp)

proc dhtFindPeersConnectedToPeer*(api: DaemonAPI, peer: PeerID,
                                 timeout = 0): Future[seq[PeerInfo]] {.async.} =
  ## Find peers which are connected to peer with id ``peer``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestDHTFindPeersConnectedToPeer(peer, timeout))
    if (res.dht.isSome()) and (res.dht.get().kind == DHTResponseType.BEGIN):
      while true:
        var message = await transp.recvMessage()
        if len(message) == 0:
          break
        var subRes = Protobuf.decode(message, SerializableDHTResponse)
        if subRes.kind == DHTResponseType.END:
          break
        if subRes.peer.isSome():
          result.add(subRes.peer.get().getPeer())
  finally:
    await api.closeConnection(transp)

proc dhtGetClosestPeers*(api: DaemonAPI, key: string,
                         timeout = 0): Future[seq[PeerID]] {.async.} =
  ## Get closest peers for ``key``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestDHTGetClosestPeers(key, timeout))
    if res.dht.isSome() and (res.dht.get().kind == DHTResponseType.BEGIN):
      while true:
        var message = await transp.recvMessage()
        let subRes = Protobuf.decode(message, SerializableDHTResponse)
        if (len(message) == 0) or (subRes.kind == DHTResponseType.END):
          break
        if subRes.value.len == 0:
          raise newException(DaemonLocalError, "Missing field `value`!")
        result.add(PeerID(data: subRes.value))
  finally:
    await api.closeConnection(transp)

proc dhtFindProviders*(api: DaemonAPI, cid: Cid, count: uint32,
                       timeout = 0): Future[seq[PeerInfo]] {.async.} =
  ## Get ``count`` providers for content with id ``cid``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  var list = newSeq[PeerInfo]()
  try:
    var res = await transp.transactMessage(requestDHTFindProviders(cid, count, timeout))
    if res.dht.isSome() and (res.dht.get().kind == DHTResponseType.BEGIN):
      while true:
        var message = await transp.recvMessage()
        let subRes = Protobuf.decode(message, SerializableDHTResponse)
        if (len(message) == 0) or (subRes.kind == DHTResponseType.END):
          break
        if subRes.peer.isSome():
          result.add(subRes.peer.get().getPeer())
  finally:
    await api.closeConnection(transp)

proc dhtSearchValue*(api: DaemonAPI, key: string,
                     timeout = 0): Future[seq[seq[byte]]] {.async.} =
  ## Search for value with ``key``, return list of values found.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  var list = newSeq[seq[byte]]()
  try:
    var res = await transp.transactMessage(requestDHTSearchValue(key, timeout))
    if res.dht.isSome() and (res.dht.get().kind == DHTResponseType.BEGIN):
      while true:
        var message = await transp.recvMessage()
        let subRes = Protobuf.decode(message, SerializableDHTResponse)
        if (len(message) == 0) or (subRes.kind == DHTResponseType.END):
          break
        result.add(subRes.value)
        if result[^1].len == 0:
          raise newException(DaemonLocalError, "Missing field `value`!")
  finally:
    await api.closeConnection(transp)

proc pubsubGetTopics*(api: DaemonAPI): Future[seq[string]] {.async.} =
  ## Get list of topics this node is subscribed to.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestPSGetTopics())
    if res.ps.isSome():
      result = res.ps.get().topics
  finally:
    await api.closeConnection(transp)

proc pubsubListPeers*(api: DaemonAPI,
                      topic: string): Future[seq[PeerID]] {.async.} =
  ## Get list of peers we are connected to and which also subscribed to topic
  ## ``topic``.
  var transp = await api.newConnection()
  try:
    var res = await transp.transactMessage(requestPSListPeers(topic))
    if res.ps.isSome():
      for id in res.ps.get().peerIDs:
        result.add(PeerID(data: id))
  finally:
    await api.closeConnection(transp)

proc pubsubPublish*(api: DaemonAPI, topic: string,
                    value: seq[byte]) {.async.} =
  ## Get list of peer identifiers which are subscribed to topic ``topic``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestPSPublish(topic, value))
  finally:
    await api.closeConnection(transp)

proc pubsubLoop(api: DaemonAPI, ticket: PubsubTicket) {.async.} =
  while true:
    var pbmessage = await ticket.transp.recvMessage()
    if len(pbmessage) == 0:
      break
    let serializable = Protobuf.decode(pbMessage, SerializablePubSubResponse)
    var message = PubSubMessage(
      peer: PeerID(data: serializable.peer),
      data: serializable.data,
      seqno: serializable.seqno,
      topics: serializable.topics
    )
    if not message.peer.validate():
      raise newException(DaemonLocalError, "Missing required field `peer`!")
    if not message.signature.init(serializable.signature):
      raise newException(DaemonLocalError, "Missing required field `signature`!")
    if not message.key.init(serializable.key):
      raise newException(DaemonLocalError, "Missing required field `key`!")
    ## We can do here `await` too
    let res = await ticket.handler(api, ticket, message)
    if not res:
      ticket.transp.close()
      await ticket.transp.join()
      break

proc pubsubSubscribe*(api: DaemonAPI, topic: string,
                   handler: P2PPubSubCallback): Future[PubsubTicket] {.async.} =
  ## Subscribe to topic ``topic``.
  var transp = await api.newConnection()
  try:
    discard await transp.transactMessage(requestPSSubscribe(topic))
    result = new PubsubTicket
    result.topic = topic
    result.handler = handler
    result.transp = transp
    asyncCheck pubsubLoop(api, result)
  except Exception as exc:
    await api.closeConnection(transp)
    raise exc

proc `$`*(pinfo: PeerInfo): string =
  ## Get string representation of ``PeerInfo`` object.
  result = newStringOfCap(128)
  result.add("{PeerID: '")
  result.add($pinfo.peer.pretty())
  result.add("' Addresses: [")
  let length = len(pinfo.addresses)
  for i in 0..<length:
    result.add("'")
    result.add($pinfo.addresses[i])
    result.add("'")
    if i < length - 1:
      result.add(", ")
  result.add("]}")
  if len(pinfo.addresses) > 0:
    result = result
