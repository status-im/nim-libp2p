## Nim-LibP2P
## Copyright (c) 2019 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.
import std/[oids, strformat]
import chronos, chronicles, stew/endians2, bearssl
import nimcrypto/[hmac, sha2, sha, hash, rijndael, twofish, bcmode]
import secure,
       ../../stream/connection,
       ../../peerinfo,
       ../../crypto/crypto,
       ../../crypto/ecnist,
       ../../peerid,
       ../../utility

export hmac, sha2, sha, hash, rijndael, bcmode

logScope:
  topics = "libp2p secio"

const
  SecioCodec* = "/secio/1.0.0"
  SecioMaxMessageSize = 8 * 1024 * 1024 ## 8mb
  SecioMaxMacSize = sha512.sizeDigest
  SecioNonceSize = 16
  SecioExchanges = "P-256,P-384,P-521"
  SecioCiphers = "TwofishCTR,AES-256,AES-128"
  SecioHashes = "SHA256,SHA512"

type
  Secio = ref object of Secure
    rng: ref BrHmacDrbgContext
    localPrivateKey: PrivateKey
    localPublicKey: PublicKey
    remotePublicKey: PublicKey

  SecureCipherType {.pure.} = enum
    Aes128, Aes256, Twofish

  SecureMacType {.pure.} = enum
    Sha1, Sha256, Sha512

  SecureCipher = object
    case kind: SecureCipherType
    of SecureCipherType.Aes128:
      ctxaes128: CTR[aes128]
    of SecureCipherType.Aes256:
      ctxaes256: CTR[aes256]
    of SecureCipherType.Twofish:
      ctxtwofish256: CTR[twofish256]

  SecureMac = object
    case kind: SecureMacType
    of SecureMacType.Sha256:
      ctxsha256: HMAC[sha256]
    of SecureMacType.Sha512:
      ctxsha512: HMAC[sha512]
    of SecureMacType.Sha1:
      ctxsha1: HMAC[sha1]

  SecioConn = ref object of SecureConn
    writerMac: SecureMac
    readerMac: SecureMac
    writerCoder: SecureCipher
    readerCoder: SecureCipher

  SecioError* = object of LPError

func shortLog*(conn: SecioConn): auto =
  if conn.isNil: "SecioConn(nil)"
  elif conn.peerInfo.isNil: $conn.oid
  else: &"{shortLog(conn.peerInfo.peerId)}:{conn.oid}"
chronicles.formatIt(SecioConn): shortLog(it)

proc init(mac: var SecureMac, hash: string, key: openarray[byte]) =
  if hash == "SHA256":
    mac = SecureMac(kind: SecureMacType.Sha256)
    mac.ctxsha256.init(key)
  elif hash == "SHA512":
    mac = SecureMac(kind: SecureMacType.Sha512)
    mac.ctxsha512.init(key)
  elif hash == "SHA1":
    mac = SecureMac(kind: SecureMacType.Sha1)
    mac.ctxsha1.init(key)

proc update(mac: var SecureMac, data: openarray[byte]) =
  case mac.kind
  of SecureMacType.Sha256:
    update(mac.ctxsha256, data)
  of SecureMacType.Sha512:
    update(mac.ctxsha512, data)
  of SecureMacType.Sha1:
    update(mac.ctxsha1, data)

proc sizeDigest(mac: SecureMac): int {.inline.} =
  case mac.kind
  of SecureMacType.Sha256:
    result = int(mac.ctxsha256.sizeDigest())
  of SecureMacType.Sha512:
    result = int(mac.ctxsha512.sizeDigest())
  of SecureMacType.Sha1:
    result = int(mac.ctxsha1.sizeDigest())

proc finish(mac: var SecureMac, data: var openarray[byte]) =
  case mac.kind
  of SecureMacType.Sha256:
    discard finish(mac.ctxsha256, data)
  of SecureMacType.Sha512:
    discard finish(mac.ctxsha512, data)
  of SecureMacType.Sha1:
    discard finish(mac.ctxsha1, data)

proc reset(mac: var SecureMac) =
  case mac.kind
  of SecureMacType.Sha256:
    reset(mac.ctxsha256)
  of SecureMacType.Sha512:
    reset(mac.ctxsha512)
  of SecureMacType.Sha1:
    reset(mac.ctxsha1)

proc init(sc: var SecureCipher, cipher: string, key: openarray[byte],
          iv: openarray[byte]) {.inline.} =
  if cipher == "AES-128":
    sc = SecureCipher(kind: SecureCipherType.Aes128)
    sc.ctxaes128.init(key, iv)
  elif cipher == "AES-256":
    sc = SecureCipher(kind: SecureCipherType.Aes256)
    sc.ctxaes256.init(key, iv)
  elif cipher == "TwofishCTR":
    sc = SecureCipher(kind: SecureCipherType.Twofish)
    sc.ctxtwofish256.init(key, iv)

proc encrypt(cipher: var SecureCipher, input: openarray[byte],
             output: var openarray[byte]) {.inline.} =
  case cipher.kind
  of SecureCipherType.Aes128:
    cipher.ctxaes128.encrypt(input, output)
  of SecureCipherType.Aes256:
    cipher.ctxaes256.encrypt(input, output)
  of SecureCipherType.Twofish:
    cipher.ctxtwofish256.encrypt(input, output)

proc decrypt(cipher: var SecureCipher, input: openarray[byte],
             output: var openarray[byte]) {.inline.} =
  case cipher.kind
  of SecureCipherType.Aes128:
    cipher.ctxaes128.decrypt(input, output)
  of SecureCipherType.Aes256:
    cipher.ctxaes256.decrypt(input, output)
  of SecureCipherType.Twofish:
    cipher.ctxtwofish256.decrypt(input, output)

proc macCheckAndDecode(sconn: SecioConn, data: var seq[byte]): bool =
  ## This procedure checks MAC of recieved message ``data`` and if message is
  ## authenticated, then decrypt message.
  ##
  ## Procedure returns ``false`` if message is too short or MAC verification
  ## failed.
  var macData: array[SecioMaxMacSize, byte]
  let macsize = sconn.readerMac.sizeDigest()
  if len(data) < macsize:
    trace "Message is shorter then MAC size", message_length = len(data),
                                              mac_size = macsize
    return false
  let mark = len(data) - macsize
  sconn.readerMac.update(data.toOpenArray(0, mark - 1))
  sconn.readerMac.finish(macData)
  sconn.readerMac.reset()
  if not equalMem(addr data[mark], addr macData[0], macsize):
    trace "Invalid MAC",
          calculated = toHex(macData.toOpenArray(0, macsize - 1)),
          stored = toHex(data.toOpenArray(mark, data.high))
    return false

  sconn.readerCoder.decrypt(data.toOpenArray(0, mark - 1),
                            data.toOpenArray(0, mark - 1))
  data.setLen(mark)
  result = true

proc readRawMessage(conn: Connection): Future[seq[byte]] {.async.} =
  while true: # Discard 0-length payloads
    var lengthBuf: array[4, byte]
    await conn.readExactly(addr lengthBuf[0], lengthBuf.len)
    let length = uint32.fromBytesBE(lengthBuf)

    trace "Recieved message header", header = lengthBuf.shortLog, length = length

    if length > SecioMaxMessageSize: # Verify length before casting!
      trace "Received size of message exceed limits", conn, length = length
      raise (ref SecioError)(msg: "Message exceeds maximum length")

    if length > 0:
      var buf = newSeq[byte](int(length))
      await conn.readExactly(addr buf[0], buf.len)
      trace "Received message body",
        conn, length = buf.len, buff = buf.shortLog
      return buf

    trace "Discarding 0-length payload", conn

method readMessage*(sconn: SecioConn): Future[seq[byte]] {.async.} =
  ## Read message from channel secure connection ``sconn``.
  when chronicles.enabledLogLevel == LogLevel.TRACE:
    logScope:
      stream_oid = $sconn.stream.oid
  var buf = await sconn.stream.readRawMessage()
  if sconn.macCheckAndDecode(buf):
    result = buf
  else:
    trace "Message MAC verification failed", buf = buf.shortLog
    raise (ref SecioError)(msg: "message failed MAC verification")

method write*(sconn: SecioConn, message: seq[byte]) {.async.} =
  ## Write message ``message`` to secure connection ``sconn``.
  if message.len == 0:
    return

  var
    left = message.len
    offset = 0
  while left > 0:
    let
      chunkSize = if left > SecioMaxMessageSize - 64: SecioMaxMessageSize - 64 else: left
      macsize = sconn.writerMac.sizeDigest()
      length = chunkSize + macsize

    var msg = newSeq[byte](chunkSize + 4 + macsize)
    msg[0..<4] = uint32(length).toBytesBE()

    sconn.writerCoder.encrypt(message.toOpenArray(offset, offset + chunkSize - 1),
                              msg.toOpenArray(4, 4 + chunkSize - 1))
    left = left - chunkSize
    offset = offset + chunkSize
    let mo = 4 + chunkSize
    sconn.writerMac.update(msg.toOpenArray(4, 4 + chunkSize - 1))
    sconn.writerMac.finish(msg.toOpenArray(mo, mo + macsize - 1))
    sconn.writerMac.reset()

    trace "Writing message", message = msg.shortLog, left, offset
    await sconn.stream.write(msg)
    sconn.activity = true

proc newSecioConn(conn: Connection,
                  hash: string,
                  cipher: string,
                  secrets: Secret,
                  order: int,
                  remotePubKey: PublicKey): SecioConn =
  ## Create new secure stream/lpstream, using specified hash algorithm ``hash``,
  ## cipher algorithm ``cipher``, stretched keys ``secrets`` and order
  ## ``order``.

  conn.peerInfo =
    if conn.peerInfo != nil:
      conn.peerInfo
    else:
      PeerInfo.init(PeerID.init(remotePubKey).tryGet())

  result = SecioConn.init(conn, conn.peerInfo, conn.observedAddr)

  let i0 = if order < 0: 1 else: 0
  let i1 = if order < 0: 0 else: 1

  trace "Writer credentials", mackey = secrets.macOpenArray(i0).shortLog,
                              enckey = secrets.keyOpenArray(i0).shortLog,
                              iv = secrets.ivOpenArray(i0).shortLog
  trace "Reader credentials", mackey = secrets.macOpenArray(i1).shortLog,
                              enckey = secrets.keyOpenArray(i1).shortLog,
                              iv = secrets.ivOpenArray(i1).shortLog
  result.writerMac.init(hash, secrets.macOpenArray(i0))
  result.readerMac.init(hash, secrets.macOpenArray(i1))
  result.writerCoder.init(cipher, secrets.keyOpenArray(i0),
                          secrets.ivOpenArray(i0))
  result.readerCoder.init(cipher, secrets.keyOpenArray(i1),
                          secrets.ivOpenArray(i1))

proc transactMessage(conn: Connection,
                     msg: seq[byte]): Future[seq[byte]] {.async.} =
  trace "Sending message", message = msg.shortLog, length = len(msg)
  await conn.write(msg)
  return await conn.readRawMessage()

method handshake*(s: Secio, conn: Connection, initiator: bool = false): Future[SecureConn] {.async.} =
  var
    localNonce: array[SecioNonceSize, byte]
    remoteNonce: seq[byte]
    remoteBytesPubkey: seq[byte]
    remoteEBytesPubkey: seq[byte]
    remoteEBytesSig: seq[byte]
    remotePubkey: PublicKey
    remoteEPubkey: ecnist.EcPublicKey
    remoteESignature: Signature
    remoteExchanges: string
    remoteCiphers: string
    remoteHashes: string
    remotePeerId: PeerID
    localPeerId: PeerID
    localBytesPubkey = s.localPublicKey.getBytes().tryGet()

  brHmacDrbgGenerate(s.rng[], localNonce)

  var request = createProposal(localNonce,
                               localBytesPubkey,
                               SecioExchanges,
                               SecioCiphers,
                               SecioHashes)

  localPeerId = PeerID.init(s.localPublicKey).tryGet()

  trace "Local proposal", schemes = SecioExchanges,
                          ciphers = SecioCiphers,
                          hashes = SecioHashes,
                          pubkey = localBytesPubkey.shortLog,
                          peer = localPeerId

  var answer = await transactMessage(conn, request)

  if len(answer) == 0:
    trace "Proposal exchange failed", conn
    raise (ref SecioError)(msg: "Proposal exchange failed")

  if not decodeProposal(answer, remoteNonce, remoteBytesPubkey, remoteExchanges,
                        remoteCiphers, remoteHashes):
    trace "Remote proposal decoding failed", conn
    raise (ref SecioError)(msg: "Remote proposal decoding failed")

  if not remotePubkey.init(remoteBytesPubkey):
    trace "Remote public key incorrect or corrupted",
          pubkey = remoteBytesPubkey.shortLog
    raise (ref SecioError)(msg: "Remote public key incorrect or corrupted")

  remotePeerId = PeerID.init(remotePubkey).tryGet()

  # TODO: PeerID check against supplied PeerID
  let order = getOrder(remoteBytesPubkey, localNonce, localBytesPubkey,
                       remoteNonce).tryGet()
  trace "Remote proposal", schemes = remoteExchanges, ciphers = remoteCiphers,
                           hashes = remoteHashes,
                           pubkey = remoteBytesPubkey.shortLog, order = order,
                           peer = remotePeerId

  let scheme = selectBest(order, SecioExchanges, remoteExchanges)
  let cipher = selectBest(order, SecioCiphers, remoteCiphers)
  let hash = selectBest(order, SecioHashes, remoteHashes)
  if len(scheme) == 0 or len(cipher) == 0 or len(hash) == 0:
    trace "No algorithms in common", peer = remotePeerId
    raise (ref SecioError)(msg: "No algorithms in common")

  trace "Encryption scheme selected", scheme = scheme, cipher = cipher,
                                      hash = hash

  var ekeypair = ephemeral(scheme, s.rng[]).tryGet()
  # We need EC public key in raw binary form
  var epubkey = ekeypair.pubkey.getRawBytes().tryGet()
  var localCorpus = request[4..^1] & answer & epubkey
  var signature = s.localPrivateKey.sign(localCorpus).tryGet()

  var localExchange = createExchange(epubkey, signature.getBytes())
  var remoteExchange = await transactMessage(conn, localExchange)
  if len(remoteExchange) == 0:
    trace "Corpus exchange failed", conn
    raise (ref SecioError)(msg: "Corpus exchange failed")

  if not decodeExchange(remoteExchange, remoteEBytesPubkey, remoteEBytesSig):
    trace "Remote exchange decoding failed", conn
    raise (ref SecioError)(msg: "Remote exchange decoding failed")

  if not remoteESignature.init(remoteEBytesSig):
    trace "Remote signature incorrect or corrupted", signature = remoteEBytesSig.shortLog
    raise (ref SecioError)(msg: "Remote signature incorrect or corrupted")

  var remoteCorpus = answer & request[4..^1] & remoteEBytesPubkey
  if not remoteESignature.verify(remoteCorpus, remotePubkey):
    trace "Signature verification failed", scheme = $remotePubkey.scheme,
                                           signature = $remoteESignature,
                                           pubkey = $remotePubkey,
                                           corpus = $remoteCorpus
    raise (ref SecioError)(msg: "Signature verification failed")

  trace "Signature verified", scheme = remotePubkey.scheme

  if not remoteEPubkey.initRaw(remoteEBytesPubkey):
    trace "Remote ephemeral public key incorrect or corrupted",
          pubkey = toHex(remoteEBytesPubkey)
    raise (ref SecioError)(msg: "Remote ephemeral public key incorrect or corrupted")

  var secret = getSecret(remoteEPubkey, ekeypair.seckey)
  if len(secret) == 0:
    trace "Shared secret could not be created"
    raise (ref SecioError)(msg: "Shared secret could not be created")

  trace "Shared secret calculated", secret = secret.shortLog

  var keys = stretchKeys(cipher, hash, secret)

  trace "Authenticated encryption parameters",
        iv0 = toHex(keys.ivOpenArray(0)), key0 = keys.keyOpenArray(0).shortLog,
        mac0 = keys.macOpenArray(0).shortLog,
        iv1 = keys.ivOpenArray(1).shortLog, key1 = keys.keyOpenArray(1).shortLog,
        mac1 = keys.macOpenArray(1).shortLog

  # Perform Nonce exchange over encrypted channel.

  var secioConn = newSecioConn(conn, hash, cipher, keys, order, remotePubkey)
  result = secioConn
  await secioConn.write(remoteNonce)
  var res = await secioConn.readMessage()

  if res != @localNonce:
    trace "Nonce verification failed", receivedNonce = res.shortLog,
                                       localNonce = localNonce.shortLog
    raise (ref SecioError)(msg: "Nonce verification failed")
  else:
    trace "Secure handshake succeeded"

method init(s: Secio) {.gcsafe.} =
  procCall Secure(s).init()
  s.codec = SecioCodec

proc newSecio*(rng: ref BrHmacDrbgContext, localPrivateKey: PrivateKey): Secio =
  result = Secio(
    rng: rng,
    localPrivateKey: localPrivateKey,
    localPublicKey: localPrivateKey.getKey().tryGet(),
  )
  result.init()
