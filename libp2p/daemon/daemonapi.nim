## Nim-LibP2P
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

## This module implementes API for `go-libp2p-daemon`.
import std/[os, osproc, strutils, tables, strtabs]
import chronos, chronicles
import chronos/selectors2 # TODO: workaround for missing `IOSelectorsException`
import ../varint, ../multiaddress, ../multicodec, ../cid, ../peerid
import ../wire, ../multihash, ../protobuf/minprotobuf, ../errors
import ../crypto/crypto

export peerid, multiaddress, multicodec, multihash, cid, crypto, wire

when not defined(windows):
  import posix

const
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
    Malformed,
    Error,
    Success

  ResponseType* {.pure.} = enum
    ERROR = 2,
    STREAMINFO = 3,
    IDENTITY = 4,
    DHT = 5,
    PEERINFO = 6
    PUBSUB = 7

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
                            stream: P2PStream): Future[void] {.gcsafe, raises: [Defect].}
  P2PPubSubCallback* = proc(api: DaemonAPI,
                            ticket: PubsubTicket,
                            message: PubSubMessage): Future[bool] {.gcsafe, raises: [Defect].}

  DaemonError* = object of LPError
  DaemonRemoteError* = object of DaemonError
  DaemonLocalError* = object of DaemonError

var daemonsCount {.threadvar.}: int

chronicles.formatIt(PeerInfo): shortLog(it)

proc requestIdentity(): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doIdentify(req *pb.Request)`.
  result = initProtoBuffer({WithVarintLength})
  result.write(initProtoField(1, cast[uint](RequestType.IDENTITY)))
  result.finish()

proc requestConnect(peerid: PeerID,
                    addresses: openarray[MultiAddress],
                    timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doConnect(req *pb.Request)`.
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, peerid))
  for item in addresses:
    msg.write(initProtoField(2, item.data.buffer))
  if timeout > 0:
    msg.write(initProtoField(3, hint64(timeout)))
  result.write(initProtoField(1, cast[uint](RequestType.CONNECT)))
  result.write(initProtoField(2, msg))
  result.finish()

proc requestDisconnect(peerid: PeerID): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doDisconnect(req *pb.Request)`.
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, peerid))
  result.write(initProtoField(1, cast[uint](RequestType.DISCONNECT)))
  result.write(initProtoField(7, msg))
  result.finish()

proc requestStreamOpen(peerid: PeerID,
                       protocols: openarray[string],
                       timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doStreamOpen(req *pb.Request)`.
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, peerid))
  for item in protocols:
    msg.write(initProtoField(2, item))
  if timeout > 0:
    msg.write(initProtoField(3, hint64(timeout)))
  result.write(initProtoField(1, cast[uint](RequestType.STREAM_OPEN)))
  result.write(initProtoField(3, msg))
  result.finish()

proc requestStreamHandler(address: MultiAddress,
                          protocols: openarray[MultiProtocol]): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doStreamHandler(req *pb.Request)`.
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, address.data.buffer))
  for item in protocols:
    msg.write(initProtoField(2, item))
  result.write(initProtoField(1, cast[uint](RequestType.STREAM_HANDLER)))
  result.write(initProtoField(4, msg))
  result.finish()

proc requestListPeers(): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/conn.go
  ## Processing function `doListPeers(req *pb.Request)`
  result = initProtoBuffer({WithVarintLength})
  result.write(initProtoField(1, cast[uint](RequestType.LIST_PEERS)))
  result.finish()

proc requestDHTFindPeer(peer: PeerID, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindPeer(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.FIND_PEER)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, peer))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTFindPeersConnectedToPeer(peer: PeerID,
                                        timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindPeersConnectedToPeer(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.FIND_PEERS_CONNECTED_TO_PEER)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, peer))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTFindProviders(cid: Cid,
                             count: uint32, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTFindProviders(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.FIND_PROVIDERS)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(3, cid.data.buffer))
  msg.write(initProtoField(6, count))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTGetClosestPeers(key: string, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetClosestPeers(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.GET_CLOSEST_PEERS)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(4, key))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTGetPublicKey(peer: PeerID, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetPublicKey(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.GET_PUBLIC_KEY)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, peer))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTGetValue(key: string, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTGetValue(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.GET_VALUE)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(4, key))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTSearchValue(key: string, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTSearchValue(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.SEARCH_VALUE)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(4, key))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTPutValue(key: string, value: openarray[byte],
                        timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTPutValue(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.PUT_VALUE)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(4, key))
  msg.write(initProtoField(5, value))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestDHTProvide(cid: Cid, timeout = 0): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/dht.go
  ## Processing function `doDHTProvide(req *pb.DHTRequest)`.
  let msgid = cast[uint](DHTRequestType.PROVIDE)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(3, cid.data.buffer))
  if timeout > 0:
    msg.write(initProtoField(7, hint64(timeout)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.DHT)))
  result.write(initProtoField(5, msg))
  result.finish()

proc requestCMTagPeer(peer: PeerID, tag: string, weight: int): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L18
  let msgid = cast[uint](ConnManagerRequestType.TAG_PEER)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, peer))
  msg.write(initProtoField(3, tag))
  msg.write(initProtoField(4, hint64(weight)))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.CONNMANAGER)))
  result.write(initProtoField(6, msg))
  result.finish()

proc requestCMUntagPeer(peer: PeerID, tag: string): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L33
  let msgid = cast[uint](ConnManagerRequestType.UNTAG_PEER)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, peer))
  msg.write(initProtoField(3, tag))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.CONNMANAGER)))
  result.write(initProtoField(6, msg))
  result.finish()

proc requestCMTrim(): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/connmgr.go#L47
  let msgid = cast[uint](ConnManagerRequestType.TRIM)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.CONNMANAGER)))
  result.write(initProtoField(6, msg))
  result.finish()

proc requestPSGetTopics(): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubGetTopics(req *pb.PSRequest)`.
  let msgid = cast[uint](PSRequestType.GET_TOPICS)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.PUBSUB)))
  result.write(initProtoField(8, msg))
  result.finish()

proc requestPSListPeers(topic: string): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubListPeers(req *pb.PSRequest)`.
  let msgid = cast[uint](PSRequestType.LIST_PEERS)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, topic))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.PUBSUB)))
  result.write(initProtoField(8, msg))
  result.finish()

proc requestPSPublish(topic: string, data: openarray[byte]): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubPublish(req *pb.PSRequest)`.
  let msgid = cast[uint](PSRequestType.PUBLISH)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, topic))
  msg.write(initProtoField(3, data))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.PUBSUB)))
  result.write(initProtoField(8, msg))
  result.finish()

proc requestPSSubscribe(topic: string): ProtoBuffer =
  ## https://github.com/libp2p/go-libp2p-daemon/blob/master/pubsub.go
  ## Processing function `doPubsubSubscribe(req *pb.PSRequest)`.
  let msgid = cast[uint](PSRequestType.SUBSCRIBE)
  result = initProtoBuffer({WithVarintLength})
  var msg = initProtoBuffer()
  msg.write(initProtoField(1, msgid))
  msg.write(initProtoField(2, topic))
  msg.finish()
  result.write(initProtoField(1, cast[uint](RequestType.PUBSUB)))
  result.write(initProtoField(8, msg))
  result.finish()

proc checkResponse(pb: var ProtoBuffer): ResponseKind {.inline.} =
  result = ResponseKind.Malformed
  var value: uint64
  if getVarintValue(pb, 1, value) > 0:
    if value == 0:
      result = ResponseKind.Success
    else:
      result = ResponseKind.Error

proc getErrorMessage(pb: var ProtoBuffer): string
  {.inline, raises: [Defect, DaemonLocalError].} =
  if pb.enterSubmessage() == cast[int](ResponseType.ERROR):
    if pb.getString(1, result) == -1:
      raise newException(DaemonLocalError, "Error message is missing!")

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
  except IOSelectorsException:
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
                   logLevel = IpfsLogLevel.Debug): Future[DaemonAPI]
                   {.async, raises: [Defect, DaemonLocalError].} =
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
  args.add("-quic=false")
  args.add("-listen=" & $api.address)

  # We are trying to get absolute daemon path.
  let cmd = findExe(daemon)
  trace "p2pd cmd", cmd, args
  if len(cmd) == 0:
    raise newException(DaemonLocalError, "Could not find daemon executable!")

  # Starting daemon process
  # echo "Starting ", cmd, " ", args.join(" ")
  try:
    api.process = startProcess(cmd, "", args, env, {poParentStreams})
  except Exception as exc:
    raiseAssert(exc.msg)

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
      let address = initTAddress(server.address).tryGet()
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
  let address = initTAddress(api.address).tryGet()
  if address.family == AddressFamily.Unix:
    discard tryRemoveFile($address)

template withMessage(m, body: untyped): untyped =
  let kind = m.checkResponse()
  if kind == ResponseKind.Error:
    raise newException(DaemonRemoteError, m.getErrorMessage())
  elif kind == ResponseKind.Malformed:
    raise newException(DaemonLocalError, "Malformed message received!")
  else:
    body

proc transactMessage(transp: StreamTransport,
                     pb: ProtoBuffer): Future[ProtoBuffer] {.async.} =
  let length = pb.getLen()
  let res = await transp.write(pb.getPtr(), length)
  if res != length:
    raise newException(DaemonLocalError, "Could not send message to daemon!")
  var message = await transp.recvMessage()
  if len(message) == 0:
    raise newException(DaemonLocalError, "Incorrect or empty message received!")
  result = initProtoBuffer(message)

proc getPeerInfo(pb: var ProtoBuffer): PeerInfo
  {.raises: [Defect, DaemonLocalError].} =
  ## Get PeerInfo object from ``pb``.
  result.addresses = newSeq[MultiAddress]()
  if pb.getValue(1, result.peer) == -1:
    raise newException(DaemonLocalError, "Missing required field `peer`!")
  var address = newSeq[byte]()
  while pb.getBytes(2, address) != -1:
    if len(address) != 0:
      var copyaddr = address
      let maRes = MultiAddress.init(copyaddr)
      if maRes.isErr:
        raise newException(DaemonLocalError, $maRes.error)

      result.addresses.add(maRes.get())
      address.setLen(0)

proc identity*(api: DaemonAPI): Future[PeerInfo] {.async.} =
  ## Get Node identity information
  var transp = await api.newConnection()
  try:
    var pb = await transactMessage(transp, requestIdentity())
    pb.withMessage() do:
      let res = pb.enterSubmessage()
      if res == cast[int](ResponseType.IDENTITY):
        result = pb.getPeerInfo()
  finally:
    await api.closeConnection(transp)

proc connect*(api: DaemonAPI, peer: PeerID,
              addresses: seq[MultiAddress],
              timeout = 0) {.async.} =
  ## Connect to remote peer with id ``peer`` and addresses ``addresses``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestConnect(peer, addresses,
                                                         timeout))
    pb.withMessage() do:
      discard
  except:
    await api.closeConnection(transp)

proc disconnect*(api: DaemonAPI, peer: PeerID) {.async.} =
  ## Disconnect from remote peer with id ``peer``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestDisconnect(peer))
    pb.withMessage() do:
      discard
  finally:
    await api.closeConnection(transp)

proc openStream*(api: DaemonAPI, peer: PeerID,
                 protocols: seq[string],
                 timeout = 0): Future[P2PStream] {.async, raises: [Defect].} =
  ## Open new stream to peer ``peer`` using one of the protocols in
  ## ``protocols``. Returns ``StreamTransport`` for the stream.
  var transp = await api.newConnection()
  var stream = new P2PStream
  try:
    var pb = await transp.transactMessage(requestStreamOpen(peer, protocols,
                                                            timeout))
    pb.withMessage() do:
      var res = pb.enterSubmessage()
      if res == cast[int](ResponseType.STREAMINFO):
        # stream.peer = newSeq[byte]()
        var raddress = newSeq[byte]()
        stream.protocol = ""
        if pb.getValue(1, stream.peer) == -1:
          raise newException(DaemonLocalError, "Missing `peer` field!")
        if pb.getLengthValue(2, raddress) == -1:
          raise newException(DaemonLocalError, "Missing `address` field!")
        stream.raddress = MultiAddress.init(raddress).tryGet()
        if pb.getLengthValue(3, stream.protocol) == -1:
          raise newException(DaemonLocalError, "Missing `proto` field!")
        stream.flags.incl(Outbound)
        stream.transp = transp
        result = stream
  except CatchableError as exc:
    await api.closeConnection(transp)
    raise exc

proc streamHandler(server: StreamServer, transp: StreamTransport) {.async.} =
  var api = getUserData[DaemonAPI](server)
  var message = await transp.recvMessage()
  var pb = initProtoBuffer(message)
  var stream = new P2PStream
  var raddress = newSeq[byte]()
  stream.protocol = ""
  if pb.getValue(1, stream.peer) == -1:
    raise newException(DaemonLocalError, "Missing `peer` field!")
  if pb.getLengthValue(2, raddress) == -1:
    raise newException(DaemonLocalError, "Missing `address` field!")
  stream.raddress = MultiAddress.init(raddress).tryGet()
  if pb.getLengthValue(3, stream.protocol) == -1:
    raise newException(DaemonLocalError, "Missing `proto` field!")
  stream.flags.incl(Inbound)
  stream.transp = transp
  if len(stream.protocol) > 0:
    var handler = api.handlers.getOrDefault(stream.protocol)
    if not isNil(handler):
      asyncSpawn handler(api, stream)

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
    var pb = await transp.transactMessage(requestStreamHandler(maddress,
                                                               protocols))
    pb.withMessage() do:
      api.servers.add(P2PServer(server: server, address: maddress))
  except CatchableError as exc:
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
    var pb = await transp.transactMessage(requestListPeers())
    pb.withMessage() do:
      result = newSeq[PeerInfo]()
      var res = pb.enterSubmessage()
      while res != 0:
        if res == cast[int](ResponseType.PEERINFO):
          var peer = pb.getPeerInfo()
          result.add(peer)
        else:
          pb.skipSubmessage()
        res = pb.enterSubmessage()
  finally:
    await api.closeConnection(transp)

proc cmTagPeer*(api: DaemonAPI, peer: PeerID, tag: string,
              weight: int) {.async.} =
  ## Tag peer with id ``peer`` using ``tag`` and ``weight``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestCMTagPeer(peer, tag, weight))
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc cmUntagPeer*(api: DaemonAPI, peer: PeerID, tag: string) {.async.} =
  ## Remove tag ``tag`` from peer with id ``peer``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestCMUntagPeer(peer, tag))
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc cmTrimPeers*(api: DaemonAPI) {.async.} =
  ## Trim all connections.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestCMTrim())
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc dhtGetSinglePeerInfo(pb: var ProtoBuffer): PeerInfo
  {.raises: [Defect, DaemonLocalError].} =
  if pb.enterSubmessage() == 2:
    result = pb.getPeerInfo()
  else:
    raise newException(DaemonLocalError, "Missing required field `peer`!")

proc dhtGetSingleValue(pb: var ProtoBuffer): seq[byte]
  {.raises: [Defect, DaemonLocalError].} =
  result = newSeq[byte]()
  if pb.getLengthValue(3, result) == -1:
    raise newException(DaemonLocalError, "Missing field `value`!")

proc dhtGetSinglePublicKey(pb: var ProtoBuffer): PublicKey
  {.raises: [Defect, DaemonLocalError].} =
  if pb.getValue(3, result) == -1:
    raise newException(DaemonLocalError, "Missing field `value`!")

proc dhtGetSinglePeerID(pb: var ProtoBuffer): PeerID
  {.raises: [Defect, DaemonLocalError].} =
  if pb.getValue(3, result) == -1:
    raise newException(DaemonLocalError, "Missing field `value`!")

proc enterDhtMessage(pb: var ProtoBuffer, rt: DHTResponseType)
  {.inline, raises: [Defect, DaemonLocalError].} =
  var dtype: uint
  var res = pb.enterSubmessage()
  if res == cast[int](ResponseType.DHT):
    if pb.getVarintValue(1, dtype) == 0:
      raise newException(DaemonLocalError, "Missing required DHT field `type`!")
    if dtype != cast[uint](rt):
      raise newException(DaemonLocalError, "Wrong DHT answer type! ")
  else:
    raise newException(DaemonLocalError, "Wrong message type!")

proc enterPsMessage(pb: var ProtoBuffer)
  {.inline, raises: [Defect, DaemonLocalError].} =
  var res = pb.enterSubmessage()
  if res != cast[int](ResponseType.PUBSUB):
    raise newException(DaemonLocalError, "Wrong message type!")

proc getDhtMessageType(pb: var ProtoBuffer): DHTResponseType
  {.inline, raises: [Defect, DaemonLocalError].} =
  var dtype: uint
  if pb.getVarintValue(1, dtype) == 0:
    raise newException(DaemonLocalError, "Missing required DHT field `type`!")
  if dtype == cast[uint](DHTResponseType.VALUE):
    result = DHTResponseType.VALUE
  elif dtype == cast[uint](DHTResponseType.END):
    result = DHTResponseType.END
  else:
    raise newException(DaemonLocalError, "Wrong DHT answer type!")

proc dhtFindPeer*(api: DaemonAPI, peer: PeerID,
                  timeout = 0): Future[PeerInfo] {.async.} =
  ## Find peer with id ``peer`` and return peer information ``PeerInfo``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestDHTFindPeer(peer, timeout))
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.VALUE)
      result = pb.dhtGetSinglePeerInfo()
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
    var pb = await transp.transactMessage(requestDHTGetPublicKey(peer, timeout))
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.VALUE)
      result = pb.dhtGetSinglePublicKey()
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
    var pb = await transp.transactMessage(requestDHTGetValue(key, timeout))
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.VALUE)
      result = pb.dhtGetSingleValue()
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
    var pb = await transp.transactMessage(requestDHTPutValue(key, value,
                                                             timeout))
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc dhtProvide*(api: DaemonAPI, cid: Cid, timeout = 0) {.async.} =
  ## Provide content with id ``cid``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestDHTProvide(cid, timeout))
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc dhtFindPeersConnectedToPeer*(api: DaemonAPI, peer: PeerID,
                                 timeout = 0): Future[seq[PeerInfo]] {.async.} =
  ## Find peers which are connected to peer with id ``peer``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  var list = newSeq[PeerInfo]()
  try:
    let spb = requestDHTFindPeersConnectedToPeer(peer, timeout)
    var pb = await transp.transactMessage(spb)
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.BEGIN)
      while true:
        var message = await transp.recvMessage()
        if len(message) == 0:
          break
        var cpb = initProtoBuffer(message)
        if cpb.getDhtMessageType() == DHTResponseType.END:
          break
        list.add(cpb.dhtGetSinglePeerInfo())
      result = list
  finally:
    await api.closeConnection(transp)

proc dhtGetClosestPeers*(api: DaemonAPI, key: string,
                         timeout = 0): Future[seq[PeerID]] {.async.} =
  ## Get closest peers for ``key``.
  ##
  ## You can specify timeout for DHT request with ``timeout`` value. ``0`` value
  ## means no timeout.
  var transp = await api.newConnection()
  var list = newSeq[PeerID]()
  try:
    let spb = requestDHTGetClosestPeers(key, timeout)
    var pb = await transp.transactMessage(spb)
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.BEGIN)
      while true:
        var message = await transp.recvMessage()
        if len(message) == 0:
          break
        var cpb = initProtoBuffer(message)
        if cpb.getDhtMessageType() == DHTResponseType.END:
          break
        list.add(cpb.dhtGetSinglePeerID())
      result = list
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
    let spb = requestDHTFindProviders(cid, count, timeout)
    var pb = await transp.transactMessage(spb)
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.BEGIN)
      while true:
        var message = await transp.recvMessage()
        if len(message) == 0:
          break
        var cpb = initProtoBuffer(message)
        if cpb.getDhtMessageType() == DHTResponseType.END:
          break
        list.add(cpb.dhtGetSinglePeerInfo())
      result = list
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
    var pb = await transp.transactMessage(requestDHTSearchValue(key, timeout))
    withMessage(pb) do:
      pb.enterDhtMessage(DHTResponseType.BEGIN)
      while true:
        var message = await transp.recvMessage()
        if len(message) == 0:
          break
        var cpb = initProtoBuffer(message)
        if cpb.getDhtMessageType() == DHTResponseType.END:
          break
        list.add(cpb.dhtGetSingleValue())
      result = list
  finally:
    await api.closeConnection(transp)

proc pubsubGetTopics*(api: DaemonAPI): Future[seq[string]] {.async.} =
  ## Get list of topics this node is subscribed to.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestPSGetTopics())
    withMessage(pb) do:
      pb.enterPsMessage()
      var topics = newSeq[string]()
      var topic = ""
      while pb.getString(1, topic) != -1:
        topics.add(topic)
        topic.setLen(0)
      result = topics
  finally:
    await api.closeConnection(transp)

proc pubsubListPeers*(api: DaemonAPI,
                      topic: string): Future[seq[PeerID]] {.async.} =
  ## Get list of peers we are connected to and which also subscribed to topic
  ## ``topic``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestPSListPeers(topic))
    withMessage(pb) do:
      var peer: PeerID
      pb.enterPsMessage()
      var peers = newSeq[PeerID]()
      while pb.getValue(2, peer) != -1:
        peers.add(peer)
      result = peers
  finally:
    await api.closeConnection(transp)

proc pubsubPublish*(api: DaemonAPI, topic: string,
                    value: seq[byte]) {.async.} =
  ## Get list of peer identifiers which are subscribed to topic ``topic``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestPSPublish(topic, value))
    withMessage(pb) do:
      discard
  finally:
    await api.closeConnection(transp)

proc getPubsubMessage*(pb: var ProtoBuffer): PubSubMessage =
  result.data = newSeq[byte]()
  result.seqno = newSeq[byte]()
  discard pb.getValue(1, result.peer)
  discard pb.getBytes(2, result.data)
  discard pb.getBytes(3, result.seqno)
  var item = newSeq[byte]()
  while true:
    if pb.getBytes(4, item) == -1:
      break
    var copyitem = item
    var stritem = cast[string](copyitem)
    if len(result.topics) == 0:
      result.topics = newSeq[string]()
    result.topics.add(stritem)
    item.setLen(0)
  discard pb.getValue(5, result.signature)
  discard pb.getValue(6, result.key)

proc pubsubLoop(api: DaemonAPI, ticket: PubsubTicket) {.async.} =
  while true:
    var pbmessage = await ticket.transp.recvMessage()
    if len(pbmessage) == 0:
      break
    var pb = initProtoBuffer(pbmessage)
    var message = pb.getPubsubMessage()
    ## We can do here `await` too
    let res = await ticket.handler(api, ticket, message)
    if not res:
      ticket.transp.close()
      await ticket.transp.join()
      break

proc pubsubSubscribe*(
  api: DaemonAPI, topic: string,
  handler: P2PPubSubCallback): Future[PubsubTicket] {.async.} =
  ## Subscribe to topic ``topic``.
  var transp = await api.newConnection()
  try:
    var pb = await transp.transactMessage(requestPSSubscribe(topic))
    pb.withMessage() do:
      var ticket = new PubsubTicket
      ticket.topic = topic
      ticket.handler = handler
      ticket.transp = transp
      asyncSpawn pubsubLoop(api, ticket)
      result = ticket
  except CatchableError as exc:
    await api.closeConnection(transp)
    raise exc

proc shortLog*(pinfo: PeerInfo): string =
  ## Get string representation of ``PeerInfo`` object.
  result = newStringOfCap(128)
  result.add("{PeerID: '")
  result.add($pinfo.peer.shortLog())
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
