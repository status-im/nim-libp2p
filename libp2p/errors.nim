# this module will be further extended in PR
# https://github.com/status-im/nim-libp2p/pull/107/

import chronos
import chronicles
import macros

# could not figure how to make it with a simple template
# sadly nim needs more love for hygenic templates
# so here goes the macro, its based on the proc/template version
# and uses quote do so it's quite readable

macro checkFutures*[T](futs: seq[Future[T]], exclude: untyped = []): untyped =
  let nexclude = exclude.len
  case nexclude
  of 0:
    quote do:
      let pos = instantiationInfo()
      for res in `futs`:
        if res.failed:
          let exc = res.readError()
          # We still don't abort but warn
          warn "Something went wrong in a future",
              error=exc.name, file=pos.filename, line=pos.line
  else:
    quote do:
      let pos = instantiationInfo()
      for res in `futs`:
        block check:
          if res.failed:
            let exc = res.readError()
            for i in 0..<`nexclude`:
              if exc of `exclude`[i]:
                trace "Ignoring an error (no warning)",
                    error=exc.name, file=pos.filename, line=pos.line
                break check
            # We still don't abort but warn
            warn "Something went wrong in a future",
                error=exc.name, file=pos.filename, line=pos.line
