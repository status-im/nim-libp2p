## Nim-LibP2P
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import chronos
import peerinfo, multiaddress, readerwriter, peerinfo

const DefaultReadSize = 1024

type
  Connection* = ref object of ReadWrite
    peerInfo*: PeerInfo
    stream: ReadWrite

proc newConnection*(stream: ReadWrite): Connection = 
  ## create a new Connection for the specified async reader/writer
  new result
  result.stream = stream

method read*(s: Connection, n = -1): Future[seq[byte]] {.async.} = 
  result = await s.stream.read(n)

method readExactly*(s: Connection, pbytes: pointer, nbytes: int): Future[void] {.async.} =
  result = s.stream.readExactly(pbytes, nbytes)

method readLine*(s: Connection, limit = 0, sep = "\r\n"): Future[string] {.async.} =
  result = await s.stream.readLine(limit, sep)

method readOnce*(s: Connection, pbytes: pointer, nbytes: int): Future[int] {.async.} =
  result = await s.stream.readOnce(pbytes, nbytes)

method readUntil*(s: Connection, pbytes: pointer, nbytes: int, sep: seq[byte]): Future[int] {.async.} =
  result = await s.stream.readUntil(pbytes, nbytes, sep)

method write*(s: Connection, pbytes: pointer, nbytes: int) {.async.} =
  result = s.stream.write(pbytes, nbytes)

method write*(s: Connection, msg: string, msglen = -1) {.async.} =
  result = s.stream.write(msg, msglen)

method write*[T](s: Connection, msg: seq[T], msglen = -1) {.async.} =
  result = s.stream.write(msg, msglen)

method close*(s: Connection) {.async.} =
  result = s.stream.close()

method getPeerInfo* (c: Connection): Future[PeerInfo] {.base, async.} = 
  ## get up to date peer info
  ## TODO: implement PeerInfo refresh over identify
  discard

method getObservedAddrs(c: Connection): Future[seq[MultiAddress]] {.base, async.} =
  ## get resolved multiaddresses for the connection
  discard
