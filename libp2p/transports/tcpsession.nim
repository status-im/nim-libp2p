import pkg/chronicles
import ../stream/chronosstream
import ./session

export session

type
  TcpSession* = ref object of Session
    stream: Connection

proc new*(_: type TcpSession,
          client: StreamTransport,
          dir: Direction,
          observedAddr: MultiAddress): TcpSession =
  let stream = Connection(
    ChronosStream.init(
      client = client,
      dir = dir,
      observedAddr = observedAddr
    )
  )
  TcpSession(stream: stream)

method getStream*(session: TcpSession): Future[Connection] {.async.} =
  result = session.stream

method close*(session: TcpSession) {.async.} =
  await session.stream.close()

method join*(session: TcpSession) {.async.} =
  await session.stream.join()

func shortLog*(session: TcpSession): string =
  if session.isNil: "TcpSession(nil)"
  else: shortLog(session.stream)
chronicles.formatIt(TcpSession): shortLog(it)
