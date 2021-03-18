import std/unittest

import chronos, bearssl

import ../libp2p/transports/tcptransport
import ../libp2p/stream/bufferstream
import ../libp2p/crypto/crypto
import ../libp2p/stream/lpstream
import ../libp2p/muxers/mplex/lpchannel
import ../libp2p/protocols/secure/secure

export unittest

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
  var tracker = getTracker(LPChannelTrackerName)
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

template asyncTeardown*(body: untyped): untyped =
  teardown:
    waitFor((
      proc() {.async, gcsafe.} =
        body
    )())

template asyncSetup*(body: untyped): untyped =
  setup:
    waitFor((
      proc() {.async, gcsafe.} =
        body
    )())

template asyncTest*(name: string, body: untyped): untyped =
  test name:
    waitFor((
      proc() {.async, gcsafe.} =
        body
    )())

template asyncCancelTest*(name: string, body: untyped): untyped =
  test name:
    var counter = 0
    while true:
      let res = waitFor((
        proc(n: int): Future[bool] {.async, gcsafe.} =
          var testIteration {.inject.} = n
          checkTrackers()
          body
      )(counter))
      if res:
        break
      inc(counter)

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
  WriteHandler* = proc(data: seq[byte]): Future[void] {.gcsafe.}
  TestBufferStream* = ref object of BufferStream
    writeHandler*: WriteHandler

method write*(s: TestBufferStream, msg: seq[byte]): Future[void] =
  s.writeHandler(msg)

proc newBufferStream*(writeHandler: WriteHandler): TestBufferStream =
  new result
  result.writeHandler = writeHandler
  result.initStream()

proc checkExpiringInternal(cond: proc(): bool): Future[bool] {.async, gcsafe.} =
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
