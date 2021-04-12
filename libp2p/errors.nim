# this module will be further extended in PR
# https://github.com/status-im/nim-libp2p/pull/107/

import chronos
import chronicles
import macros

type
  # Base exception type for libp2p
  LPError* = object of CatchableError
  LPAllFuturesError* = object of LPError
    errors*: seq[ref CatchableError]

# could not figure how to make it with a simple template
# sadly nim needs more love for hygienic templates
# so here goes the macro, its based on the proc/template version
# and uses quote do so it's quite readable

macro checkFutures*[T](futs: seq[Future[T]], exclude: untyped = []): untyped =
  let nexclude = exclude.len
  case nexclude
  of 0:
    quote do:
      for res in `futs`:
        if res.failed:
          let exc = res.readError()
          # We still don't abort but warn
          debug "A future has failed, enable trace logging for details", error = exc.name
          trace "Exception message", msg= exc.msg, stack = getStackTrace()
  else:
    quote do:
      for res in `futs`:
        block check:
          if res.failed:
            let exc = res.readError()
            for i in 0..<`nexclude`:
              if exc of `exclude`[i]:
                trace "A future has failed", error=exc.name, msg=exc.msg
                break check
            # We still don't abort but warn
            debug "A future has failed, enable trace logging for details", error=exc.name
            trace "Exception details", msg=exc.msg

proc allFuturesThrowing*[T](args: varargs[Future[T]]): Future[void]
  {.raises: [Defect, LPAllFuturesError, CancelledError].} =
  var futs: seq[Future[T]]
  for fut in args:
    futs &= fut

  proc call() {.async.} =
    var allErrors = new LPAllFuturesError
    futs = await allFinished(futs)
    for fut in futs:
      if fut.failed:
        let err = fut.readError()
        if err of Defect:
          raise err
        else:
          if err of CancelledError:
            raise err
          if isNil(err):
            allErrors.errors.add(err)

    if allErrors.errors.len > 0:
      raise allErrors

  return call()

template tryAndWarn*(message: static[string]; body: untyped): untyped =
  try:
    body
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "An exception has ocurred, enable trace logging for details", name = exc.name, msg = message
    trace "Exception details", exc = exc.msg
