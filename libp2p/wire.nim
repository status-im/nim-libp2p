## Nim-Libp2p
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

## This module implements wire network connection procedures.
import chronos, stew/endians2
import multiaddress, multicodec, errors

when defined(windows):
  import winlean
else:
  import posix

const
  RTRANSPMA* = mapOr(
    TCP,
    WebSockets,
    UNIX
  )

  TRANSPMA* = mapOr(
    RTRANSPMA,
    UDP
  )


proc initTAddress*(ma: MultiAddress): MaResult[TransportAddress] =
  ## Initialize ``TransportAddress`` with MultiAddress ``ma``.
  ##
  ## MultiAddress must be wire address, e.g. ``{IP4, IP6, UNIX}/{TCP, UDP}``.
  ##

  if TRANSPMA.match(ma):
    var pbuf: array[2, byte]
    let code = (?(?ma[0]).protoCode())
    if code == multiCodec("unix"):
      var res = TransportAddress(family: AddressFamily.Unix)
      if (?(?ma[0]).protoArgument(res.address_un)) == 0:
        err("Incorrect Unix domain address")
      else:
        res.port = Port(1)
        ok(res)
    elif code == multiCodec("ip4"):
      var res = TransportAddress(family: AddressFamily.IPv4)
      if (?(?ma[0]).protoArgument(res.address_v4)) == 0:
        err("Incorrect IPv4 address")
      else:
        if (?(?ma[1]).protoArgument(pbuf)) == 0:
          err("Incorrect port number")
        else:
          res.port = Port(fromBytesBE(uint16, pbuf))
          ok(res)
    else:
      var res = TransportAddress(family: AddressFamily.IPv6)
      if (?(?ma[0]).protoArgument(res.address_v6)) == 0:
        err("Incorrect IPv6 address")
      else:
        if (?(?ma[1]).protoArgument(pbuf)) == 0:
          err("Incorrect port number")
        else:
          res.port = Port(fromBytesBE(uint16, pbuf))
          ok(res)
  else:
    err("MultiAddress must be wire address (tcp, udp or unix)")

proc connect*(
  ma: MultiAddress,
  bufferSize = DefaultStreamBufferSize,
  child: StreamTransport = nil): Future[StreamTransport]
  {.raises: [Defect, LPError, MaInvalidAddress].} =
  ## Open new connection to remote peer with address ``ma`` and create
  ## new transport object ``StreamTransport`` for established connection.
  ## ``bufferSize`` is size of internal buffer for transport.
  ##

  if not(RTRANSPMA.match(ma)):
    raise newException(MaInvalidAddress, "Incorrect or unsupported address!")

  return connect(initTAddress(ma).tryGet(), bufferSize, child)

proc createStreamServer*[T](ma: MultiAddress,
                            cbproc: StreamCallback,
                            flags: set[ServerFlags] = {},
                            udata: ref T,
                            sock: AsyncFD = asyncInvalidSocket,
                            backlog: int = 100,
                            bufferSize: int = DefaultStreamBufferSize,
                            child: StreamServer = nil,
                            init: TransportInitCallback = nil): StreamServer
                            {.raises: [Defect, LPError, MaInvalidAddress].} =
  ## Create new TCP stream server which bounds to ``ma`` address.
  if not(RTRANSPMA.match(ma)):
    raise newException(MaInvalidAddress, "Incorrect or unsupported address!")

  try:
    return createStreamServer(
      initTAddress(ma).tryGet(),
      cbproc,
      flags,
      udata,
      sock,
      backlog,
      bufferSize,
      child,
      init)
  except CatchableError as exc:
    raise newException(LPError, exc.msg)

proc createStreamServer*[T](ma: MultiAddress,
                            flags: set[ServerFlags] = {},
                            udata: ref T,
                            sock: AsyncFD = asyncInvalidSocket,
                            backlog: int = 100,
                            bufferSize: int = DefaultStreamBufferSize,
                            child: StreamServer = nil,
                            init: TransportInitCallback = nil): StreamServer
                            {.raises: [Defect, LPError, MaInvalidAddress].} =
  ## Create new TCP stream server which bounds to ``ma`` address.
  ##

  if not(RTRANSPMA.match(ma)):
    raise newException(MaInvalidAddress, "Incorrect or unsupported address!")

  try:
    return createStreamServer(
      initTAddress(ma).tryGet(),
      flags,
      udata,
      sock,
      backlog,
      bufferSize,
      child,
      init)
  except CatchableError as exc:
    raise newException(LPError, exc.msg)

proc createAsyncSocket*(ma: MultiAddress): AsyncFD
  {.raises: [Defect, LPError].} =
  ## Create new asynchronous socket using MultiAddress' ``ma`` socket type and
  ## protocol information.
  ##
  ## Returns ``asyncInvalidSocket`` on error.
  ##
  ## Note: This procedure only used in `go-libp2p-daemon` wrapper.
  ##

  var
    socktype: SockType = SockType.SOCK_STREAM
    protocol: Protocol = Protocol.IPPROTO_TCP

  let address = initTAddress(ma).tryGet()
  if address.family in {AddressFamily.IPv4, AddressFamily.IPv6}:
    if ma[1].tryGet().protoCode().tryGet() == multiCodec("udp"):
      socktype = SockType.SOCK_DGRAM
      protocol = Protocol.IPPROTO_UDP
    elif ma[1].tryGet().protoCode().tryGet() == multiCodec("tcp"):
      socktype = SockType.SOCK_STREAM
      protocol = Protocol.IPPROTO_TCP
  elif address.family in {AddressFamily.Unix}:
    socktype = SockType.SOCK_STREAM
    protocol = cast[Protocol](0)
  else:
    return asyncInvalidSocket

  try:
    createAsyncSocket(address.getDomain(), socktype, protocol)
  except CatchableError as exc:
    raise newException(LPError, exc.msg)

proc bindAsyncSocket*(sock: AsyncFD, ma: MultiAddress): bool
  {.raises: [Defect, LPError].} =
  ## Bind socket ``sock`` to MultiAddress ``ma``.
  ##
  ## Note: This procedure only used in `go-libp2p-daemon` wrapper.
  ##

  var
    saddr: Sockaddr_storage
    slen: SockLen

  let address = initTAddress(ma).tryGet()
  toSAddr(address, saddr, slen)
  if bindSocket(SocketHandle(sock), cast[ptr SockAddr](addr saddr),
                slen) == 0:
    result = true
  else:
    result = false

proc getLocalAddress*(sock: AsyncFD): TransportAddress =
  ## Retrieve local socket ``sock`` address.
  ##
  ## Note: This procedure only used in `go-libp2p-daemon` wrapper.
  var saddr: Sockaddr_storage
  var slen = SockLen(sizeof(Sockaddr_storage))

  if getsockname(SocketHandle(sock), cast[ptr SockAddr](addr saddr),
                 addr slen) == 0:
    fromSAddr(addr saddr, slen, result)
