import strutils, strformat, stew/byteutils
import chronos
import ../libp2p/errors,
       ../libp2p/multistream,
       ../libp2p/stream/bufferstream,
       ../libp2p/stream/connection,
       ../libp2p/multiaddress,
       ../libp2p/transports/transport,
       ../libp2p/transports/tcptransport,
       ../libp2p/protocols/protocol,
       ../libp2p/upgrademngrs/upgrade

import ./helpers
import ./asyncunit

when defined(nimHasUsed): {.used.}

## Mock stream for select test
type
  TestSelectStream = ref object of Connection
    step*: int

method readOnce*(
  s: TestSelectStream,
  pbytes: pointer,
  nbytes: int): Future[int]
  {.async, raises: [Defect, LPStreamEOFError].} =
  case s.step:
    of 1:
      var buf = newSeq[byte](1)
      buf[0] = 19
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 2
      return buf.len
    of 2:
      var buf = "/multistream/1.0.0\n"
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 3
      return buf.len
    of 3:
      var buf = newSeq[byte](1)
      buf[0] = 18
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 4
      return buf.len
    of 4:
      var buf = "/test/proto/1.0.0\n"
      copyMem(pbytes, addr buf[0], buf.len())
      return buf.len
    else:
      copyMem(pbytes,
              cstring("\0x3na\n"),
              "\0x3na\n".len())

      return "\0x3na\n".len()

method write*(s: TestSelectStream, msg: seq[byte])
  {.async, raises: [Defect, LPStreamClosedError].} =
  discard

method close(s: TestSelectStream) {.async, gcsafe, raises: [Defect].} =
  s.isClosed = true
  s.isEof = true

proc newTestSelectStream(): TestSelectStream =
  new result
  result.step = 1

## Mock stream for handles `ls` test
type
  LsHandler = proc(procs: seq[byte]): Future[void]
    {.gcsafe, raises: [Defect].}

  TestLsStream = ref object of Connection
    step*: int
    ls*: LsHandler

method readOnce*(
  s: TestLsStream,
  pbytes: pointer,
  nbytes: int): Future[int]
  {.async, raises: [Defect, LPStreamEOFError].} =
  case s.step:
    of 1:
      var buf = newSeq[byte](1)
      buf[0] = 19
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 2
      return buf.len()
    of 2:
      var buf = "/multistream/1.0.0\n"
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 3
      return buf.len()
    of 3:
      var buf = newSeq[byte](1)
      buf[0] = 3
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 4
      return buf.len()
    of 4:
      var buf = "ls\n"
      copyMem(pbytes, addr buf[0], buf.len())
      return buf.len()
    else:
      var buf = "na\n"
      copyMem(pbytes, addr buf[0], buf.len())
      return buf.len()

method write*(s: TestLsStream, msg: seq[byte])
  {.async, raises: [Defect, LPStreamClosedError].} =
  if s.step == 4:
    await s.ls(msg)

method close(s: TestLsStream) {.async, gcsafe.} =
  s.isClosed = true
  s.isEof = true

proc newTestLsStream(ls: LsHandler): TestLsStream {.gcsafe.} =
  new result
  result.ls = ls
  result.step = 1

## Mock stream for handles `na` test
type
  NaHandler = proc(procs: string): Future[void]
    {.gcsafe, raises: [Defect].}

  TestNaStream = ref object of Connection
    step*: int
    na*: NaHandler

method readOnce*(
  s: TestNaStream,
  pbytes: pointer,
  nbytes: int): Future[int]
  {.async, raises: [Defect, LPStreamEOFError].} =
  case s.step:
    of 1:
      var buf = newSeq[byte](1)
      buf[0] = 19
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 2
      return buf.len()
    of 2:
      var buf = "/multistream/1.0.0\n"
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 3
      return buf.len()
    of 3:
      var buf = newSeq[byte](1)
      buf[0] = 18
      copyMem(pbytes, addr buf[0], buf.len())
      s.step = 4
      return buf.len()
    of 4:
      var buf = "/test/proto/1.0.0\n"
      copyMem(pbytes, addr buf[0], buf.len())
      return buf.len()
    else:
      copyMem(pbytes,
              cstring("\0x3na\n"),
              "\0x3na\n".len())

      return "\0x3na\n".len()

method write*(s: TestNaStream, msg: seq[byte])
  {.async, raises: [Defect, LPStreamClosedError].} =
  if s.step == 4:
    await s.na(string.fromBytes(msg))

method close(s: TestNaStream) {.async, gcsafe.} =
  s.isClosed = true
  s.isEof = true

proc newTestNaStream(na: NaHandler): TestNaStream =
  new result
  result.na = na
  result.step = 1

suite "Multistream select":
  teardown:
    checkTrackers()

  asyncTest "test select custom proto":
    let ms = newMultistream()
    let conn = newTestSelectStream()
    check (await ms.select(conn, @["/test/proto/1.0.0"])) == "/test/proto/1.0.0"
    await conn.close()

  asyncTest "test handle custom proto":
    let ms = newMultistream()
    let conn = newTestSelectStream()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async, gcsafe.} =
      check proto == "/test/proto/1.0.0"
      await conn.close()

    protocol.handler = testHandler
    ms.addHandler("/test/proto/1.0.0", protocol)
    await ms.handle(conn)

  asyncTest "test handle `ls`":
    let ms = newMultistream()

    var conn: Connection
    let done = newFuture[void]()
    proc testLsHandler(proto: seq[byte]) {.async, gcsafe, raises: [Defect].} =
      var strProto: string = string.fromBytes(proto)
      check strProto == "\x26/test/proto1/1.0.0\n/test/proto2/1.0.0\n"
      await conn.close()
      done.complete()

    conn = Connection(newTestLsStream(testLsHandler))

    proc testHandler(conn: Connection, proto: string)
      {.async, gcsafe, raises: [Defect].} = discard

    var protocol: LPProtocol = new LPProtocol
    protocol.handler = testHandler
    ms.addHandler("/test/proto1/1.0.0", protocol)
    ms.addHandler("/test/proto2/1.0.0", protocol)
    await ms.handle(conn)
    await done.wait(5.seconds)

  asyncTest "test handle `na`":
    let ms = newMultistream()

    var conn: Connection
    proc testNaHandler(msg: string): Future[void]
      {.async, gcsafe, raises: [Defect].} =
      echo msg
      check msg == Na
      await conn.close()
    conn = newTestNaStream(testNaHandler)

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async, gcsafe.} = discard
    protocol.handler = testHandler
    ms.addHandler("/unabvailable/proto/1.0.0", protocol)

    await ms.handle(conn)

  asyncTest "e2e - handle":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async, gcsafe.} =
      check proto == "/test/proto/1.0.0"
      await conn.writeLp("Hello!")
      await conn.close()

    protocol.handler = testHandler
    let msListen = newMultistream()
    msListen.addHandler("/test/proto/1.0.0", protocol)

    let transport1 = TcpTransport.init(upgrade = Upgrade())
    asyncSpawn transport1.start(ma)

    proc acceptHandler(): Future[void] {.async, gcsafe.} =
      let conn = await transport1.accept()
      await msListen.handle(conn)
      await conn.close()

    let handlerWait = acceptHandler()

    let msDial = newMultistream()
    let transport2 = TcpTransport.init(upgrade = Upgrade())
    let conn = await transport2.dial(transport1.ma)

    check (await msDial.select(conn, "/test/proto/1.0.0")) == true

    let hello = string.fromBytes(await conn.readLp(1024))
    check hello == "Hello!"
    await conn.close()

    await transport2.stop()
    await transport1.stop()

    await handlerWait.wait(30.seconds)

  asyncTest "e2e - ls":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    let
      handlerWait = newFuture[void]()

    let msListen = newMultistream()
    var protocol: LPProtocol = new LPProtocol
    protocol.handler = proc(conn: Connection, proto: string) {.async, gcsafe.} =
      # never reached
      discard

    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async.} =
      # never reached
      discard

    protocol.handler = testHandler
    msListen.addHandler("/test/proto1/1.0.0", protocol)
    msListen.addHandler("/test/proto2/1.0.0", protocol)

    let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    let listenFut = transport1.start(ma)

    proc acceptHandler(): Future[void] {.async, gcsafe.} =
      let conn = await transport1.accept()
      try:
        await msListen.handle(conn)
      except LPStreamEOFError:
        discard
      except LPStreamClosedError:
        discard
      finally:
        await conn.close()

    let acceptFut = acceptHandler()
    let msDial = newMultistream()
    let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    let conn = await transport2.dial(transport1.ma)

    let ls = await msDial.list(conn)
    let protos: seq[string] = @["/test/proto1/1.0.0", "/test/proto2/1.0.0"]

    check ls == protos

    await conn.close()
    await acceptFut
    await transport2.stop()
    await transport1.stop()
    await listenFut.wait(5.seconds)

  asyncTest "e2e - select one from a list with unsupported protos":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async, gcsafe.} =
      check proto == "/test/proto/1.0.0"
      await conn.writeLp("Hello!")
      await conn.close()

    protocol.handler = testHandler
    let msListen = newMultistream()
    msListen.addHandler("/test/proto/1.0.0", protocol)

    let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    asyncSpawn transport1.start(ma)

    proc acceptHandler(): Future[void] {.async, gcsafe.} =
      let conn = await transport1.accept()
      await msListen.handle(conn)

    let acceptFut = acceptHandler()
    let msDial = newMultistream()
    let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    let conn = await transport2.dial(transport1.ma)

    check (await msDial.select(conn,
      @["/test/proto/1.0.0", "/test/no/proto/1.0.0"])) == "/test/proto/1.0.0"

    let hello = string.fromBytes(await conn.readLp(1024))
    check hello == "Hello!"

    await conn.close()
    await acceptFut
    await transport2.stop()
    await transport1.stop()

  asyncTest "e2e - select one with both valid":
    let ma: MultiAddress = Multiaddress.init("/ip4/0.0.0.0/tcp/0").tryGet()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(conn: Connection,
                      proto: string):
                      Future[void] {.async, gcsafe.} =
      await conn.writeLp(&"Hello from {proto}!")
      await conn.close()

    protocol.handler = testHandler
    let msListen = newMultistream()
    msListen.addHandler("/test/proto1/1.0.0", protocol)
    msListen.addHandler("/test/proto2/1.0.0", protocol)

    let transport1: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    asyncSpawn transport1.start(ma)

    proc acceptHandler(): Future[void] {.async, gcsafe.} =
      let conn = await transport1.accept()
      await msListen.handle(conn)

    let acceptFut = acceptHandler()
    let msDial = newMultistream()
    let transport2: TcpTransport = TcpTransport.init(upgrade = Upgrade())
    let conn = await transport2.dial(transport1.ma)

    check (await msDial.select(conn,
      @[
        "/test/proto2/1.0.0",
        "/test/proto1/1.0.0"
      ])) == "/test/proto2/1.0.0"

    check string.fromBytes(await conn.readLp(1024)) == "Hello from /test/proto2/1.0.0!"

    await conn.close()
    await acceptFut
    await transport2.stop()
    await transport1.stop()
