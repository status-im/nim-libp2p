## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[tables,
            options,
            sets,
            oids,
            sugar,
            math]

import chronos,
       chronicles,
       metrics

import stream/connection,
       transports/transport,
       upgrademngrs/[upgrade, muxedupgrade],
       multistream,
       multiaddress,
       protocols/protocol,
       protocols/secure/secure,
       peerinfo,
       protocols/identify,
       muxers/muxer,
       utils/semaphore,
       connmanager,
       peerid,
       errors,
       dialer

export connmanager, upgrade, dialer

logScope:
  topics = "libp2p switch"

#TODO: General note - use a finite state machine to manage the different
# steps of connections establishing and upgrading. This makes everything
# more robust and less prone to ordering attacks - i.e. muxing can come if
# and only if the channel has been secured (i.e. if a secure manager has been
# previously provided)

declareCounter(libp2p_failed_upgrades_incoming, "incoming connections failed upgrades")

const
  ConcurrentUpgrades* = 4

type
    Switch* = ref object of Dial
      peerInfo*: PeerInfo
      connManager*: ConnManager
      transports*: seq[Transport]
      ms*: MultistreamSelect
      acceptFuts: seq[Future[void]]
      dialer*: Dial

template addConnEventHandler*(
  s: Switch,
  handler: ConnEventHandler,
  kind: ConnEventKind) =
  s.connManager.addConnEventHandler(handler, kind)

template removeConnEventHandler*(
  s: Switch,
  handler: ConnEventHandler,
  kind: ConnEventKind) =
  s.connManager.removeConnEventHandler(handler, kind)

template addPeerEventHandler*(
  s: Switch,
  handler: PeerEventHandler,
  kind: PeerEventKind) =
  s.connManager.addPeerEventHandler(handler, kind)

template removePeerEventHandler*(
  s: Switch,
  handler: PeerEventHandler,
  kind: PeerEventKind) =
  s.connManager.removePeerEventHandler(handler, kind)

proc isConnected*(s: Switch, peerId: PeerID): bool =
  ## returns true if the peer has one or more
  ## associated connections (sockets)
  ##

  peerId in s.connManager

proc disconnect*(s: Switch, peerId: PeerID): Future[void] {.gcsafe.} =
  s.connManager.dropPeer(peerId)

method connect*(
  s: Switch,
  peerId: PeerID,
  addrs: seq[MultiAddress]): Future[void]
  {.raises: [Defect, DialFailedError].} =
  s.dialer.connect(peerId, addrs)

method dial*(
  s: Switch,
  peerId: PeerID,
  protos: seq[string]): Future[Connection]
  {.raises: [Defect, DialFailedError].} =
  s.dialer.dial(peerId, protos)

proc dial*(s: Switch,
           peerId: PeerID,
           proto: string): Future[Connection]
  {.raises: [Defect, DialFailedError].} =
  dial(s, peerId, @[proto])

method dial*(
  s: Switch,
  peerId: PeerID,
  addrs: seq[MultiAddress],
  protos: seq[string]): Future[Connection]
  {.raises: [Defect, DialFailedError].} =
  s.dialer.dial(peerId, addrs, protos)

proc dial*(
  s: Switch,
  peerId: PeerID,
  addrs: seq[MultiAddress],
  proto: string): Future[Connection]
  {.raises: [Defect, DialFailedError].} =
  dial(s, peerId, addrs, @[proto])

proc mount*[T: LPProtocol](s: Switch, proto: T, matcher: Matcher = nil) {.gcsafe, raises: [Defect, LPError].} =
  if isNil(proto.handler):
    raise newException(LPError,
      "Protocol has to define a handle method or proc")

  if proto.codec.len == 0:
    raise newException(LPError,
      "Protocol has to define a codec string")

  s.ms.addHandler(proto.codecs, proto, matcher)
  s.peerInfo.protocols.add(proto.codec)

proc upgradeMonitor(conn: Connection, upgrades: AsyncSemaphore) {.async.} =
  ## monitor connection for upgrades
  ##
  try:
    # Since we don't control the flow of the
    # upgrade, this timeout guarantees that a
    # "hanged" remote doesn't hold the upgrade
    # forever
    await conn.onUpgrade.wait(30.seconds) # wait for connection to be upgraded
    trace "Connection upgrade succeeded"
  except CatchableError as exc:
    libp2p_failed_upgrades_incoming.inc()
    if not isNil(conn):
      await conn.close()

    trace "Exception awaiting connection upgrade", exc = exc.msg, conn
  finally:
    upgrades.release() # don't forget to release the slot!

proc accept(s: Switch, transport: Transport) {.async.} = # noraises
  ## switch accept loop, ran for every transport
  ##

  let upgrades = newAsyncSemaphore(ConcurrentUpgrades)
  while transport.running:
    var conn: Connection
    try:
      debug "About to accept incoming connection"
      # remember to always release the slot when
      # the upgrade succeeds or fails, this is
      # currently done by the `upgradeMonitor`
      await upgrades.acquire()    # first wait for an upgrade slot to become available
      conn = await s.connManager  # next attempt to get an incoming connection
      .trackIncomingConn(
        () => transport.accept()
      )
      if isNil(conn):
        # A nil connection means that we might have hit a
        # file-handle limit (or another non-fatal error),
        # we can get one on the next try, but we should
        # be careful to not end up in a thigh loop that
        # will starve the main event loop, thus we sleep
        # here before retrying.
        trace "Unable to get a connection, sleeping"
        await sleepAsync(100.millis) # TODO: should be configurable?
        upgrades.release()
        continue

      # set the direction of this bottom level transport
      # in order to be able to consume this information in gossipsub if required
      # gossipsub gives priority to connections we make
      conn.transportDir = Direction.In

      debug "Accepted an incoming connection", conn
      asyncSpawn upgradeMonitor(conn, upgrades)
      asyncSpawn transport.upgradeIncoming(conn)
    except CancelledError as exc:
      trace "releasing semaphore on cancellation"
      upgrades.release() # always release the slot
    except CatchableError as exc:
      debug "Exception in accept loop, exiting", exc = exc.msg
      upgrades.release() # always release the slot
      if not isNil(conn):
        await conn.close()
      return

proc start*(s: Switch): Future[seq[Future[void]]] {.async, gcsafe.} =
  trace "starting switch for peer", peerInfo = s.peerInfo
  var startFuts: seq[Future[void]]
  for t in s.transports: # for each transport
    for i, a in s.peerInfo.addrs:
      if t.handles(a): # check if it handles the multiaddr
        var server = t.start(a)
        s.peerInfo.addrs[i] = t.ma # update peer's address
        s.acceptFuts.add(s.accept(t))
        startFuts.add(server)

  debug "Started libp2p node", peer = s.peerInfo
  return startFuts # listen for incoming connections

proc stop*(s: Switch) {.async.} =
  trace "Stopping switch"

  # close and cleanup all connections
  await s.connManager.close()

  for t in s.transports:
    try:
      await t.stop()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "error cleaning up transports", msg = exc.msg

  try:
    await allFuturesThrowing(
      allFinished(s.acceptFuts)).wait(1.seconds)
  except CatchableError as exc:
    trace "Exception while stopping accept loops", exc = exc.msg

  # check that all futures were properly
  # stopped and otherwise cancel them
  for a in s.acceptFuts:
    if not a.finished:
      a.cancel()

  trace "Switch stopped"

proc newSwitch*(peerInfo: PeerInfo,
                transports: seq[Transport],
                identity: Identify,
                muxers: Table[string, MuxerProvider],
                secureManagers: openarray[Secure] = [],
                connManager: ConnManager,
                ms: MultistreamSelect): Switch
                {.raises: [Defect, LPError].} =
  if secureManagers.len == 0:
    raise newException(LPError, "Provide at least one secure manager")

  let switch = Switch(
    peerInfo: peerInfo,
    ms: ms,
    transports: transports,
    connManager: connManager,
    dialer: Dialer.new(peerInfo, connManager, transports, ms))

  switch.mount(identity)
  return switch

# proc isConnected*(s: Switch, peerInfo: PeerInfo): bool
#   {.deprecated: "Use PeerID version".} =
#   not isNil(peerInfo) and isConnected(s, peerInfo.peerId)

# proc disconnect*(s: Switch, peerInfo: PeerInfo): Future[void]
#   {.deprecated: "Use PeerID version", gcsafe.} =
#   disconnect(s, peerInfo.peerId)

# proc connect*(s: Switch, peerInfo: PeerInfo): Future[void]
#   {.deprecated: "Use PeerID version".} =
#   connect(s, peerInfo.peerId, peerInfo.addrs)

# proc dial*(s: Switch,
#            peerInfo: PeerInfo,
#            proto: string):
#            Future[Connection]
#   {.deprecated: "Use PeerID version".} =
#   dial(s, peerInfo.peerId, peerInfo.addrs, proto)
