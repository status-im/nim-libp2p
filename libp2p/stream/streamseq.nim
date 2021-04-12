{.push raises: [Defect].}

import stew/bitops2

type
  StreamSeq* = object
    # Seq adapted to the stream use case where we add data at the back and
    # consume at the front in chunks. A bit like a deque but contiguous memory
    # area - will try to avoid moving data unless it has to, subject to buffer
    # space. The assumption is that data is typically consumed fully.
    #
    # See also asio::stream_buf

    buf: seq[byte] # Data store
    rpos: int # Reading position - valid data starts here
    wpos: int # Writing position - valid data ends here

template len*(v: StreamSeq): int =
  v.wpos - v.rpos

func grow(v: var StreamSeq, n: int) =
  if v.rpos == v.wpos:
    # All data has been consumed, reset positions
    v.rpos = 0
    v.wpos = 0

  if v.buf.len - v.wpos < n:
    if v.rpos > 0:
      # We've consumed some data so we'll try to move that data to the beginning
      # of the buffer, hoping that this will clear up enough capacity to avoid
      # reallocation
      moveMem(addr v.buf[0], addr v.buf[v.rpos], v.wpos - v.rpos)
      v.wpos -= v.rpos
      v.rpos = 0

      if v.buf.len - v.wpos >= n:
        return

    # TODO this is inefficient - `setLen` will copy all data of buf, even though
    #      we know that only a part of it contains "valid" data
    v.buf.setLen(nextPow2(max(64, v.wpos + n).uint64).int)

template prepare*(v: var StreamSeq, n: int): var openArray[byte] =
  ## Return a buffer that is at least `n` bytes long
  mixin grow
  v.grow(n)

  v.buf.toOpenArray(v.wpos, v.buf.len - 1)

template commit*(v: var StreamSeq, n: int) =
  ## Mark `n` bytes in the buffer returned by `prepare` as ready for reading
  v.wpos += n

func add*(v: var StreamSeq, data: openArray[byte]) =
  ## Add data - the equivalent of `buf.prepare(n) = data; buf.commit(n)`
  if data.len > 0:
    v.grow(data.len)
    copyMem(addr v.buf[v.wpos], unsafeAddr data[0], data.len)
    v.commit(data.len)

template data*(v: StreamSeq): openArray[byte] =
  # Data that is ready to be consumed
  # TODO a double-hash comment here breaks compile (!)
  v.buf.toOpenArray(v.rpos, v.wpos - 1)

template toOpenArray*(v: StreamSeq, b, e: int): openArray[byte] =
  # Data that is ready to be consumed
  # TODO a double-hash comment here breaks compile (!)
  v.buf.toOpenArray(v.rpos + b, v.rpos + e - b)

func consume*(v: var StreamSeq, n: int) =
  ## Mark `n` bytes that were returned via `data` as consumed
  v.rpos += n

func consumeTo*(v: var StreamSeq, buf: var openArray[byte]): int =
  let bytes = min(buf.len, v.len)
  if bytes > 0:
    copyMem(addr buf[0], addr v.buf[v.rpos], bytes)
    v.consume(bytes)
  bytes

func clear*(v: var StreamSeq) =
  v.consume(v.len)

func assign*(v: var StreamSeq, buf: openArray[byte]) =
  v.clear()
  v.add(buf)
