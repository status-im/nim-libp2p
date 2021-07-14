## Nim-Libp2p
## Copyright (c) 2018 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not BE copied, modified, or distributed except according to
## those terms.

## This module implements MultiCodec.

{.push raises: [Defect].}

import tables, hashes
import varint, vbuffer
import stew/results
export results

{.deadCodeElim: on.}

## List of officially supported codecs can BE found here
## https://github.com/multiformats/multicodec/blob/master/table.csv
const MultiCodecList = [
  ("raw", 0x55),
  # serialization formats
  ("cbor", 0x51),
  ("protobuf", 0x50),
  ("rlp", 0x60),
  ("bencode", 0x63),
  # multiformats
  ("multicodec", 0x30),
  ("multihash", 0x31),
  ("multiaddr", 0x32),
  ("multibase", 0x33),
  # multihashes
  ("identity", 0x00),
  ("md4", 0xD4),
  ("md5", 0xD5),
  ("sha1", 0x11),
  ("sha2-256", 0x12),
  ("sha2-512", 0x13),
  ("dbl-sha2-256", 0x56),
  ("sha3-224", 0x17),
  ("sha3-256", 0x16),
  ("sha3-384", 0x15),
  ("sha3-512", 0x14),
  ("shake-128", 0x18),
  ("shake-256", 0x19),
  ("keccak-224", 0x1A),
  ("keccak-256", 0x1B),
  ("keccak-384", 0x1C),
  ("keccak-512", 0x1D),
  ("murmur3", 0x22),
  ("blake2b-8", 0xB201), ("blake2b-16", 0xB202), ("blake2b-24", 0xB203),
  ("blake2b-32", 0xB204), ("blake2b-40", 0xB205), ("blake2b-48", 0xB206),
  ("blake2b-56", 0xB207), ("blake2b-64", 0xB208), ("blake2b-72", 0xB209),
  ("blake2b-80", 0xB20A), ("blake2b-88", 0xB20B), ("blake2b-96", 0xB20C),
  ("blake2b-104", 0xB20D), ("blake2b-112", 0xB20E), ("blake2b-120", 0xB20F),
  ("blake2b-128", 0xB210), ("blake2b-136", 0xB211), ("blake2b-144", 0xB212),
  ("blake2b-152", 0xB213), ("blake2b-160", 0xB214), ("blake2b-168", 0xB215),
  ("blake2b-176", 0xB216), ("blake2b-184", 0xB217), ("blake2b-192", 0xB218),
  ("blake2b-200", 0xB219), ("blake2b-208", 0xB21A), ("blake2b-216", 0xB21B),
  ("blake2b-224", 0xB21C), ("blake2b-232", 0xB21D), ("blake2b-240", 0xB21E),
  ("blake2b-248", 0xB21F), ("blake2b-256", 0xB220), ("blake2b-264", 0xB221),
  ("blake2b-272", 0xB222), ("blake2b-280", 0xB223), ("blake2b-288", 0xB224),
  ("blake2b-296", 0xB225), ("blake2b-304", 0xB226), ("blake2b-312", 0xB227),
  ("blake2b-320", 0xB228), ("blake2b-328", 0xB229), ("blake2b-336", 0xB22A),
  ("blake2b-344", 0xB22B), ("blake2b-352", 0xB22C), ("blake2b-360", 0xB22D),
  ("blake2b-368", 0xB22E), ("blake2b-376", 0xB22F), ("blake2b-384", 0xB230),
  ("blake2b-392", 0xB231), ("blake2b-400", 0xB232), ("blake2b-408", 0xB233),
  ("blake2b-416", 0xB234), ("blake2b-424", 0xB235), ("blake2b-432", 0xB236),
  ("blake2b-440", 0xB237), ("blake2b-448", 0xB238), ("blake2b-456", 0xB239),
  ("blake2b-464", 0xB23A), ("blake2b-472", 0xB23B), ("blake2b-480", 0xB23C),
  ("blake2b-488", 0xB23D), ("blake2b-496", 0xB23E), ("blake2b-504", 0xB23F),
  ("blake2b-512", 0xB240), ("blake2s-8", 0xB241), ("blake2s-16", 0xB242),
  ("blake2s-24", 0xB243), ("blake2s-32", 0xB244), ("blake2s-40", 0xB245),
  ("blake2s-48", 0xB246), ("blake2s-56", 0xB247), ("blake2s-64", 0xB248),
  ("blake2s-72", 0xB249), ("blake2s-80", 0xB24A), ("blake2s-88", 0xB24B),
  ("blake2s-96", 0xB24C), ("blake2s-104", 0xB24D), ("blake2s-112", 0xB24E),
  ("blake2s-120", 0xB24F), ("blake2s-128", 0xB250), ("blake2s-136", 0xB251),
  ("blake2s-144", 0xB252), ("blake2s-152", 0xB253), ("blake2s-160", 0xB254),
  ("blake2s-168", 0xB255), ("blake2s-176", 0xB256), ("blake2s-184", 0xB257),
  ("blake2s-192", 0xB258), ("blake2s-200", 0xB259), ("blake2s-208", 0xB25A),
  ("blake2s-216", 0xB25B), ("blake2s-224", 0xB25C), ("blake2s-232", 0xB25D),
  ("blake2s-240", 0xB25E), ("blake2s-248", 0xB25F), ("blake2s-256", 0xB260),
  ("skein256-8", 0xB301), ("skein256-16", 0xB302), ("skein256-24", 0xB303),
  ("skein256-32", 0xB304), ("skein256-40", 0xB305), ("skein256-48", 0xB306),
  ("skein256-56", 0xB307), ("skein256-64", 0xB308), ("skein256-72", 0xB309),
  ("skein256-80", 0xB30A), ("skein256-88", 0xB30B), ("skein256-96", 0xB30C),
  ("skein256-104", 0xB30D), ("skein256-112", 0xB30E), ("skein256-120", 0xB30F),
  ("skein256-128", 0xB310), ("skein256-136", 0xB311), ("skein256-144", 0xB312),
  ("skein256-152", 0xB313), ("skein256-160", 0xB314), ("skein256-168", 0xB315),
  ("skein256-176", 0xB316), ("skein256-184", 0xB317), ("skein256-192", 0xB318),
  ("skein256-200", 0xB319), ("skein256-208", 0xB31A), ("skein256-216", 0xB31B),
  ("skein256-224", 0xB31C), ("skein256-232", 0xB31D), ("skein256-240", 0xB31E),
  ("skein256-248", 0xB31F), ("skein256-256", 0xB320),
  ("skein512-8", 0xB321), ("skein512-16", 0xB322), ("skein512-24", 0xB323),
  ("skein512-32", 0xB324), ("skein512-40", 0xB325), ("skein512-48", 0xB326),
  ("skein512-56", 0xB327), ("skein512-64", 0xB328), ("skein512-72", 0xB329),
  ("skein512-80", 0xB32A), ("skein512-88", 0xB32B), ("skein512-96", 0xB32C),
  ("skein512-104", 0xB32D), ("skein512-112", 0xB32E), ("skein512-120", 0xB32F),
  ("skein512-128", 0xB330), ("skein512-136", 0xB331), ("skein512-144", 0xB332),
  ("skein512-152", 0xB333), ("skein512-160", 0xB334), ("skein512-168", 0xB335),
  ("skein512-176", 0xB336), ("skein512-184", 0xB337), ("skein512-192", 0xB338),
  ("skein512-200", 0xB339), ("skein512-208", 0xB33A), ("skein512-216", 0xB33B),
  ("skein512-224", 0xB33C), ("skein512-232", 0xB33D), ("skein512-240", 0xB33E),
  ("skein512-248", 0xB33F), ("skein512-256", 0xB340), ("skein512-264", 0xB341),
  ("skein512-272", 0xB342), ("skein512-280", 0xB343), ("skein512-288", 0xB344),
  ("skein512-296", 0xB345), ("skein512-304", 0xB346), ("skein512-312", 0xB347),
  ("skein512-320", 0xB348), ("skein512-328", 0xB349), ("skein512-336", 0xB34A),
  ("skein512-344", 0xB34B), ("skein512-352", 0xB34C), ("skein512-360", 0xB34D),
  ("skein512-368", 0xB34E), ("skein512-376", 0xB34F), ("skein512-384", 0xB350),
  ("skein512-392", 0xB351), ("skein512-400", 0xB352), ("skein512-408", 0xB353),
  ("skein512-416", 0xB354), ("skein512-424", 0xB355), ("skein512-432", 0xB356),
  ("skein512-440", 0xB357), ("skein512-448", 0xB358), ("skein512-456", 0xB359),
  ("skein512-464", 0xB35A), ("skein512-472", 0xB35B), ("skein512-480", 0xB35C),
  ("skein512-488", 0xB35D), ("skein512-496", 0xB35E), ("skein512-504", 0xB35F),
  ("skein512-512", 0xB360), ("skein1024-8", 0xB361), ("skein1024-16", 0xB362),
  ("skein1024-24", 0xB363), ("skein1024-32", 0xB364), ("skein1024-40", 0xB365),
  ("skein1024-48", 0xB366), ("skein1024-56", 0xB367), ("skein1024-64", 0xB368),
  ("skein1024-72", 0xB369), ("skein1024-80", 0xB36A), ("skein1024-88", 0xB36B),
  ("skein1024-96", 0xB36C), ("skein1024-104", 0xB36D),
  ("skein1024-112", 0xB36E), ("skein1024-120", 0xB36F),
  ("skein1024-128", 0xB370), ("skein1024-136", 0xB371),
  ("skein1024-144", 0xB372), ("skein1024-152", 0xB373),
  ("skein1024-160", 0xB374), ("skein1024-168", 0xB375),
  ("skein1024-176", 0xB376), ("skein1024-184", 0xB377),
  ("skein1024-192", 0xB378), ("skein1024-200", 0xB379),
  ("skein1024-208", 0xB37A), ("skein1024-216", 0xB37B),
  ("skein1024-224", 0xB37C), ("skein1024-232", 0xB37D),
  ("skein1024-240", 0xB37E), ("skein1024-248", 0xB37F),
  ("skein1024-256", 0xB380), ("skein1024-264", 0xB381),
  ("skein1024-272", 0xB382), ("skein1024-280", 0xB383),
  ("skein1024-288", 0xB384), ("skein1024-296", 0xB385),
  ("skein1024-304", 0xB386), ("skein1024-312", 0xB387),
  ("skein1024-320", 0xB388), ("skein1024-328", 0xB389),
  ("skein1024-336", 0xB38A), ("skein1024-344", 0xB38B),
  ("skein1024-352", 0xB38C), ("skein1024-360", 0xB38D),
  ("skein1024-368", 0xB38E), ("skein1024-376", 0xB38F),
  ("skein1024-384", 0xB390), ("skein1024-392", 0xB391),
  ("skein1024-400", 0xB392), ("skein1024-408", 0xB393),
  ("skein1024-416", 0xB394), ("skein1024-424", 0xB395),
  ("skein1024-432", 0xB396), ("skein1024-440", 0xB397),
  ("skein1024-448", 0xB398), ("skein1024-456", 0xB399),
  ("skein1024-464", 0xB39A), ("skein1024-472", 0xB39B),
  ("skein1024-480", 0xB39C), ("skein1024-488", 0xB39D),
  ("skein1024-496", 0xB39E), ("skein1024-504", 0xB39F),
  ("skein1024-512", 0xB3A0), ("skein1024-520", 0xB3A1),
  ("skein1024-528", 0xB3A2), ("skein1024-536", 0xB3A3),
  ("skein1024-544", 0xB3A4), ("skein1024-552", 0xB3A5),
  ("skein1024-560", 0xB3A6), ("skein1024-568", 0xB3A7),
  ("skein1024-576", 0xB3A8), ("skein1024-584", 0xB3A9),
  ("skein1024-592", 0xB3AA), ("skein1024-600", 0xB3AB),
  ("skein1024-608", 0xB3AC), ("skein1024-616", 0xB3AD),
  ("skein1024-624", 0xB3AE), ("skein1024-632", 0xB3AF),
  ("skein1024-640", 0xB3B0), ("skein1024-648", 0xB3B1),
  ("skein1024-656", 0xB3B2), ("skein1024-664", 0xB3B3),
  ("skein1024-672", 0xB3B4), ("skein1024-680", 0xB3B5),
  ("skein1024-688", 0xB3B6), ("skein1024-696", 0xB3B7),
  ("skein1024-704", 0xB3B8), ("skein1024-712", 0xB3B9),
  ("skein1024-720", 0xB3BA), ("skein1024-728", 0xB3BB),
  ("skein1024-736", 0xB3BC), ("skein1024-744", 0xB3BD),
  ("skein1024-752", 0xB3BE), ("skein1024-760", 0xB3BF),
  ("skein1024-768", 0xB3C0), ("skein1024-776", 0xB3C1),
  ("skein1024-784", 0xB3C2), ("skein1024-792", 0xB3C3),
  ("skein1024-800", 0xB3C4), ("skein1024-808", 0xB3C5),
  ("skein1024-816", 0xB3C6), ("skein1024-824", 0xB3C7),
  ("skein1024-832", 0xB3C8), ("skein1024-840", 0xB3C9),
  ("skein1024-848", 0xB3CA), ("skein1024-856", 0xB3CB),
  ("skein1024-864", 0xB3CC), ("skein1024-872", 0xB3CD),
  ("skein1024-880", 0xB3CE), ("skein1024-888", 0xB3CF),
  ("skein1024-896", 0xB3D0), ("skein1024-904", 0xB3D1),
  ("skein1024-912", 0xB3D2), ("skein1024-920", 0xB3D3),
  ("skein1024-928", 0xB3D4), ("skein1024-936", 0xB3D5),
  ("skein1024-944", 0xB3D6), ("skein1024-952", 0xB3D7),
  ("skein1024-960", 0xB3D8), ("skein1024-968", 0xB3D9),
  ("skein1024-976", 0xB3DA), ("skein1024-984", 0xB3DB),
  ("skein1024-992", 0xB3DC), ("skein1024-1000", 0xB3DD),
  ("skein1024-1008", 0xB3DE), ("skein1024-1016", 0xB3DF),
  ("skein1024-1024", 0xB3E0),
  # multiaddrs
  ("ip4", 0x04),
  ("ip6", 0x29),
  ("ip6zone", 0x2A),
  ("tcp", 0x06),
  ("udp", 0x0111),
  ("dccp", 0x21),
  ("sctp", 0x84),
  ("udt", 0x012D),
  ("utp", 0x012E),
  ("unix", 0x0190), # not in multicodec list
  ("ipfs", 0x01A5),
  ("p2p", 0x01A5),
  ("http", 0x01E0),
  ("https", 0x01BB),
  ("quic", 0x01CC),
  ("ws", 0x01DD),
  ("wss", 0x01DE), # not in multicodec list
  ("p2p-websocket-star", 0x01DF), # not in multicodec list
  ("p2p-webrtc-star", 0x0113), # not in multicodec list
  ("p2p-webrtc-direct", 0x0114), # not in multicodec list
  ("onion", 0x01BC),
  ("p2p-circuit", 0x0122),
  ("dns", 0x35),
  ("dns4", 0x36),
  ("dns6", 0x37),
  ("dnsaddr", 0x38),
  # IPLD formats
  ("dag-pb", 0x70),
  ("dag-cbor", 0x71),
  ("dag-json", 0x129),
  ("git-raw", 0x78),
  ("eth-block", 0x90),
  ("eth-block-list", 0x91),
  ("eth-tx-trie", 0x92),
  ("eth-tx", 0x93),
  ("eth-tx-receipt-trie", 0x94),
  ("eth-tx-receipt", 0x95),
  ("eth-state-trie", 0x96),
  ("eth-account-snapshot", 0x97),
  ("eth-storage-trie", 0x98),
  ("bitcoin-block", 0xB0),
  ("bitcoin-tx", 0xB1),
  ("zcash-block", 0xC0),
  ("zcash-tx", 0xC1),
  ("stellar-block", 0xD0),
  ("stellar-tx", 0xD1),
  ("decred-block", 0xE0),
  ("decred-tx", 0xE1),
  ("dash-block", 0xF0),
  ("dash-tx", 0xF1),
  ("torrent-info", 0x7B),
  ("torrent-file", 0x7C),
  ("ed25519-pub", 0xED)
]

type
  MultiCodec* = distinct int
  MultiCodecError* = enum
    MultiCodecNotSupported

const
  InvalidMultiCodec* = MultiCodec(-1)

proc initMultiCodecNameTable(): Table[string, int] {.compileTime.} =
  for item in MultiCodecList:
    result[item[0]] = item[1]

proc initMultiCodecCodeTable(): Table[int, string] {.compileTime.} =
  for item in MultiCodecList:
    result[item[1]] = item[0]

const
  CodeCodecs = initMultiCodecCodeTable()
  NameCodecs = initMultiCodecNameTable()

proc multiCodec*(name: string): MultiCodec {.compileTime.} =
  ## Generate MultiCodec from string ``name`` at compile time.
  let code = NameCodecs.getOrDefault(name, -1)
  doAssert(code != -1)
  MultiCodec(code)

proc multiCodec*(code: int): MultiCodec {.compileTime.} =
  ## Generate MultiCodec from integer ``code`` at compile time.
  let name = CodeCodecs.getOrDefault(code, "")
  doAssert(name != "")
  MultiCodec(code)

proc `$`*(mc: MultiCodec): string =
  ## Returns string representation of MultiCodec ``mc``.
  let name = CodeCodecs.getOrDefault(int(mc), "")
  doAssert(name != "")
  name

proc `==`*(mc: MultiCodec, name: string): bool {.inline.} =
  ## Compares MultiCodec ``mc`` with string ``name``.
  let mcname = CodeCodecs.getOrDefault(int(mc), "")
  if mcname == "":
    return false
  result = (mcname == name)

proc `==`*(mc: MultiCodec, code: int): bool {.inline.} =
  ## Compares MultiCodec ``mc`` with integer ``code``.
  (int(mc) == code)

proc `==`*(a, b: MultiCodec): bool =
  ## Returns ``true`` if MultiCodecs ``a`` and ``b`` are equal.
  int(a) == int(b)

proc `!=`*(a, b: MultiCodec): bool =
  ## Returns ``true`` if MultiCodecs ``a`` and ``b`` are not equal.
  int(a) != int(b)

proc hash*(m: MultiCodec): Hash {.inline.} =
  ## Hash procedure for tables.
  hash(int(m))

proc codec*(mt: typedesc[MultiCodec], name: string): MultiCodec {.inline.} =
  ## Return MultiCodec from string representation ``name``.
  ## If ``name`` is not valid multicodec name, then ``InvalidMultiCodec`` will
  ## be returned.
  MultiCodec(NameCodecs.getOrDefault(name, -1))

proc codec*(mt: typedesc[MultiCodec], code: int): MultiCodec {.inline.} =
  ## Return MultiCodec from integer representation ``code``.
  ## If ``code`` is not valid multicodec code, then ``InvalidMultiCodec`` will
  ## be returned.
  let res = CodeCodecs.getOrDefault(code, "")
  if res == "":
    InvalidMultiCodec
  else:
    MultiCodec(code)

proc write*(vb: var VBuffer, mc: MultiCodec) {.inline.} =
  ## Write MultiCodec to buffer ``vb``.
  vb.writeVarint(cast[uint](mc))
