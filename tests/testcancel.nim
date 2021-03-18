{.used.}

import unittest, options, sequtils
import chronos, chronicles
import stew/byteutils
import nimcrypto/sysrand
import ../libp2p/[errors,
                  switch,
                  multistream,
                  standard_setup,
                  stream/bufferstream,
                  stream/connection,
                  multiaddress,
                  peerinfo,
                  crypto/crypto,
                  protocols/protocol,
                  protocols/secure/secure,
                  muxers/muxer,
                  muxers/mplex/lpchannel,
                  stream/lpstream,
                  stream/chronosstream,
                  transports/tcptransport]
import ./helpers

const
  TestCodec = "/test/proto/1.0.0"

type
  TestProto = ref object of LPProtocol

suite "Cancellation test suite":
  teardown:
    checkTrackers()

  asyncCancelTest "e2e switch dial cancellation test":
    var
      res = false
      awaiters: seq[Future[void]]
      abnormalFailures = 0
      abnormalFinishes = 0


    proc handle(conn: Connection, proto: string) {.async, gcsafe.} =
      try:
        let msg = string.fromBytes(await conn.readLp(1024))
        await conn.writeLp("Hello!")
      finally:
        await conn.close()


    let testProto = TestProto(codecs: @[TestCodec], handler: handle)
    let switch1 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    switch1.mount(testProto)
    let switch2 = newStandardSwitch(secureManagers = [SecureProtocol.Noise])
    awaiters.add(await switch1.start())
    awaiters.add(await switch2.start())

    # We are testing `switch.dial` procedure.
    notice "=== Test iteration ", iteration = testIteration
    let connFut = switch2.dial(switch1.peerInfo, TestCodec)
    if connFut.finished():
      notice "=== Future immediately finished", state = $connFut.state
      # Future was finished immediately, its impossible to cancel such Future.
      if connFut.done():
        await connFut.read().close()
      res = true
    else:
      notice "=== Future is not finished, after procedure call, canceling",
             state = $connFut.state
      # Future is in `Pending` state.
      if testIteration == 0:
        connFut.cancel()
      else:
        # Now we waiting N number of asynchronous steps (poll() calls).
        await stepsAsync(testIteration)
        if connFut.finished():
          notice "=== Future was finished after waiting number of poll calls",
                 count = (testIteration + 1), state = $connFut.state
          # Future was finished, so our test is finished.
          if connFut.done():
            await connFut.read().close()
          else:
            assert(false)
          res = true
        else:
          connFut.cancel()

    if not(res):
      if not(connFut.finished()):
        notice "=== Future was not finished right after cancellation",
               state = $connFut.state
        check: await connFut.withTimeout(1.seconds)
        notice "=== Future state after waiting for completion",
               state = $connFut.state
        case connFut.state
        of FutureState.Finished:
          notice "=== Future finished with result",
                 place = $(connFut.location[LocCompleteIndex])
          await connFut.read().close()
          inc abnormalFinishes
        of FutureState.Failed:
          let exc = connFut.error
          notice "=== Future finished with an exception", name = $exc.name,
                 msg = $exc.msg,
                 place = $(connFut.location[LocCompleteIndex])
          inc abnormalFailures
        of FutureState.Cancelled:
          notice "=== Future was cancelled"
        of FutureState.Pending:
          notice "=== Future is still pending after cancellation," &
                 "it could stuck or timeout is too small for this Future"
      else:
        notice "=== Future was finished right after cancellation",
               state = $connFut.state
        if connFut.done():
          await connFut.read().close()

    # if any of those happened cancellation was not properly propagated to chronos
    check:
      abnormalFailures == 0
      abnormalFinishes == 0

    await allFuturesThrowing(
      switch1.stop(),
      switch2.stop()
    )

    # this needs to go at end
    await allFuturesThrowing(awaiters)

    return res
