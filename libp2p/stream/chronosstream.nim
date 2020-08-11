## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import oids
import chronos, chronicles
import connection

logScope:
  topics = "chronosstream"

const
  DefaultChronosStreamTimeout = 10.minutes

type
  ChronosStream* = ref object of Connection
    client: StreamTransport

method initStream*(s: ChronosStream) =
  if s.objName.len == 0:
    s.objName = "ChronosStream"

  s.timeoutHandler = proc() {.async, gcsafe.} =
    trace "idle timeout expired, closing ChronosStream"
    await s.close()

  procCall Connection(s).initStream()

proc init*(C: type ChronosStream,
           client: StreamTransport,
           timeout = DefaultChronosStreamTimeout): ChronosStream =
  result = C(client: client,
             timeout: timeout)
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

method readOnce*(s: ChronosStream, pbytes: pointer, nbytes: int): Future[int] {.async.} =
  if s.atEof:
    raise newLPStreamEOFError()

  withExceptions:
    result = await s.client.readOnce(pbytes, nbytes)
    s.activity = true # reset activity flag

method write*(s: ChronosStream, msg: seq[byte]) {.async.} =
  if s.closed:
    raise newLPStreamClosedError()

  if msg.len == 0:
    return

  withExceptions:
    var written = 0
    while not s.client.closed and written < msg.len:
      written += await s.client.write(msg[written..<msg.len])
      s.activity = true # reset activity flag

    if written < msg.len:
      raise (ref LPStreamClosedError)(msg: "Write couldn't finish writing")

method closed*(s: ChronosStream): bool {.inline.} =
  result = s.client.closed

method atEof*(s: ChronosStream): bool {.inline.} =
  s.client.atEof()

method close*(s: ChronosStream) {.async.} =
  try:
    if not s.isClosed:
      trace "shutting down chronos stream", address = $s.client.remoteAddress(),
                                            oid = $s.oid
      if not s.client.closed():
        await s.client.closeWait()

      await procCall Connection(s).close()
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "error closing chronosstream", exc = exc.msg
