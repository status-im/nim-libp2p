import pkg/chronos
import ../stream/connection

export chronos
export connection

type
  Session* = ref object of RootObj

method getStream*(session: Session): Future[Connection] {.async, base.} =
  doAssert false # not implemented

method close*(session: Session) {.async, base.} =
  doAssert false # not implemented

method join*(session: Session) {.async, base.} =
  doAssert false # not implemented
