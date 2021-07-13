{.push raises: [Defect].}

import chronos, bearssl

import ../libp2p/transports/tcptransport
import ../libp2p/stream/bufferstream
import ../libp2p/crypto/crypto
import ../libp2p/stream/lpstream
import ../libp2p/stream/chronosstream
import ../libp2p/muxers/mplex/lpchannel
import ../libp2p/protocols/secure/secure

import ./asyncunit
export asyncunit

const
  StreamTransportTrackerName = "stream.transport"
  StreamServerTrackerName = "stream.server"

  trackerNames = [
    LPStreamTrackerName,
    ConnectionTrackerName,
    LPChannelTrackerName,
    SecureConnTrackerName,
    BufferStreamTrackerName,
    TcpTransportTrackerName,
    StreamTransportTrackerName,
    ChronosStreamTrackerName,
    StreamServerTrackerName
  ]

iterator testTrackers*(extras: openArray[string] = []): TrackerBase =
  for name in trackerNames:
    let t = getTracker(name)
    if not isNil(t): yield t
  for name in extras:
    let t = getTracker(name)
    if not isNil(t): yield t

template checkTracker*(name: string) =
  var tracker = getTracker(name)
  if tracker.isLeaked():
    checkpoint tracker.dump()
    fail()

template checkTrackers*() =
  for tracker in testTrackers():
    if tracker.isLeaked():
      checkpoint tracker.dump()
      fail()
  # Also test the GC is not fooling with us
  GC_fullCollect()

type RngWrap = object
  rng: ref BrHmacDrbgContext

var rngVar: RngWrap

proc getRng(): ref BrHmacDrbgContext =
  # TODO if `rngVar` is a threadvar like it should be, there are random and
  #      spurious compile failures on mac - this is not gcsafe but for the
  #      purpose of the tests, it's ok as long as we only use a single thread
  {.gcsafe.}:
    if rngVar.rng.isNil:
      rngVar.rng = newRng()
    rngVar.rng

template rng*(): ref BrHmacDrbgContext =
  getRng()

type
  WriteHandler* = proc(data: seq[byte]): Future[void] {.gcsafe, raises: [Defect].}
  TestBufferStream* = ref object of BufferStream
    writeHandler*: WriteHandler

method write*(s: TestBufferStream, msg: seq[byte]): Future[void] =
  s.writeHandler(msg)

proc new*(T: typedesc[TestBufferStream], writeHandler: WriteHandler): T =
  let testBufferStream = T(writeHandler: writeHandler)
  testBufferStream.initStream()
  testBufferStream

proc newBufferStream*(writeHandler: WriteHandler): TestBufferStream {.deprecated: "use TestBufferStream.new".}=
  TestBufferStream.new(writeHandler)

proc checkExpiringInternal(cond: proc(): bool {.raises: [Defect].} ): Future[bool] {.async, gcsafe.} =
  {.gcsafe.}:
    let start = Moment.now()
    while true:
      if Moment.now() > (start + chronos.seconds(5)):
        return false
      elif cond():
        return true
      else:
        await sleepAsync(1.millis)

template checkExpiring*(code: untyped): untyped =
  checkExpiringInternal(proc(): bool = code)
