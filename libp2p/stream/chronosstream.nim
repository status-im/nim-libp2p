## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[oids, strformat]
import chronos, chronicles, metrics
import connection
import ../utility

logScope:
  topics = "libp2p chronosstream"

const
  DefaultChronosStreamTimeout = 10.minutes
  ChronosStreamTrackerName* = "ChronosStream"

type
  ChronosStream* = ref object of Connection
    client: StreamTransport
    when defined(libp2p_agents_metrics):
      tracked: bool
      shortAgent: string

when defined(libp2p_agents_metrics):
  declareGauge(libp2p_peers_identity, "peers identities", labels = ["agent"])
  declareCounter(libp2p_peers_traffic_read, "incoming traffic", labels = ["agent"])
  declareCounter(libp2p_peers_traffic_write, "outgoing traffic", labels = ["agent"])

func shortLog*(conn: ChronosStream): auto =
  try:
    if conn.isNil: "ChronosStream(nil)"
    elif conn.peerInfo.isNil: $conn.oid
    else: &"{shortLog(conn.peerInfo.peerId)}:{conn.oid}"
  except ValueError as exc:
    raiseAssert(exc.msg)

chronicles.formatIt(ChronosStream): shortLog(it)

method initStream*(s: ChronosStream) =
  if s.objName.len == 0:
    s.objName = "ChronosStream"

  s.timeoutHandler = proc() {.async, gcsafe.} =
    trace "Idle timeout expired, closing ChronosStream", s
    await s.close()

  procCall Connection(s).initStream()

proc init*(C: type ChronosStream,
           client: StreamTransport,
           dir: Direction,
           timeout = DefaultChronosStreamTimeout,
           observedAddr: MultiAddress = MultiAddress()): ChronosStream =
  result = C(client: client,
             timeout: timeout,
             dir: dir,
             observedAddr: observedAddr)
  result.initStream()

template withExceptions(body: untyped) =
  try:
    body
  except CancelledError as exc:
    raise exc
  except TransportIncompleteError:
    # for all intents and purposes this is an EOF
    raise newLPStreamIncompleteError()
  except TransportLimitError:
    raise newLPStreamLimitError()
  except TransportUseClosedError:
    raise newLPStreamEOFError()
  except TransportError:
    # TODO https://github.com/status-im/nim-chronos/pull/99
    raise newLPStreamEOFError()

when defined(libp2p_agents_metrics):
  proc trackPeerIdentity(s: ChronosStream) =
    if not s.tracked:
      if not isNil(s.peerInfo) and s.peerInfo.agentVersion.len > 0:
        # / seems a weak "standard" so for now it's reliable
        let shortAgent = s.peerInfo.agentVersion.split("/")[0].safeToLowerAscii()
        if shortAgent.isOk() and KnownLibP2PAgentsSeq.contains(shortAgent.get()):
          s.shortAgent = shortAgent.get()
        else:
          s.shortAgent = "unknown"
        libp2p_peers_identity.inc(labelValues = [s.shortAgent])
        s.tracked = true

  proc untrackPeerIdentity(s: ChronosStream) =
    if s.tracked:
      libp2p_peers_identity.dec(labelValues = [s.shortAgent])
      s.tracked = false

method readOnce*(s: ChronosStream, pbytes: pointer, nbytes: int): Future[int] {.async.} =
  if s.atEof:
    raise newLPStreamEOFError()

  withExceptions:
    result = await s.client.readOnce(pbytes, nbytes)
    s.activity = true # reset activity flag
    when defined(libp2p_agents_metrics):
      s.trackPeerIdentity()
      if s.tracked:
        libp2p_peers_traffic_read.inc(nbytes.int64, labelValues = [s.shortAgent])

method write*(s: ChronosStream, msg: seq[byte])
  {.async, raises: [Defect, LPStreamClosedError].} =

  if s.closed:
    raise newLPStreamClosedError()

  if msg.len == 0:
    return

  withExceptions:
    # StreamTransport will only return written < msg.len on fatal failures where
    # further writing is not possible - in such cases, we'll raise here,
    # since we don't return partial writes lengths
    var written = await s.client.write(msg)

    if written < msg.len:
      raise (ref LPStreamClosedError)(msg: "Write couldn't finish writing")

    s.activity = true # reset activity flag
    when defined(libp2p_agents_metrics):
      s.trackPeerIdentity()
      if s.tracked:
        libp2p_peers_traffic_write.inc(msg.len.int64, labelValues = [s.shortAgent])

method closed*(s: ChronosStream): bool =
  result = s.client.closed

method atEof*(s: ChronosStream): bool =
  s.client.atEof()

method closeImpl*(s: ChronosStream) {.async.} =
  try:
    trace "Shutting down chronos stream", address = $s.client.remoteAddress(), s

    if not s.client.closed():
      await s.client.closeWait()

    trace "Shutdown chronos stream", address = $s.client.remoteAddress(), s

  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "Error closing chronosstream", s, msg = exc.msg

  when defined(libp2p_agents_metrics):
    # do this after closing!
    s.untrackPeerIdentity()

  await procCall Connection(s).closeImpl()
