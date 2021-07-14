import unittest2
import ../libp2p/[multicodec, multiaddress]

when defined(nimHasUsed): {.used.}

type
  PatternVector = object
    pattern: MaPattern
    good: seq[string]
    bad: seq[string]

const
  SuccessVectors = [
    "/ip4/1.2.3.4",
    "/ip4/0.0.0.0",
    "/ip6/::1",
    "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21",
    "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21/udp/1234/quic",
    "/ip6zone/x/ip6/fe80::1",
    "/ip6zone/x%y/ip6/fe80::1",
    "/ip6zone/x%y/ip6/::",
    "/ip6zone/x/ip6/fe80::1/udp/1234/quic",
    "/onion/timaq4ygg2iegci7:1234",
    "/onion/timaq4ygg2iegci7:80/http",
    "/udp/0",
    "/tcp/0",
    "/sctp/0",
    "/udp/1234",
    "/tcp/1234",
    "/sctp/1234",
    "/udp/65535",
    "/tcp/65535",
    "/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/udp/1234/sctp/1234",
    "/udp/1234/udt",
    "/udp/1234/utp",
    "/tcp/1234/http",
    "/tcp/1234/https",
    "/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/ip4/127.0.0.1/udp/1234",
    "/ip4/127.0.0.1/udp/0",
    "/ip4/127.0.0.1/tcp/1234",
    "/ip4/127.0.0.1/tcp/1234/",
    "/ip4/127.0.0.1/udp/1234/quic",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/unix/a/b/c/d/e",
    "/unix/stdio",
    "/ip4/1.2.3.4/tcp/80/unix/a/b/c/d/e/f",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234/unix/stdio",
    "/ip4/127.0.0.1/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234/unix/stdio",
    "/dns/example.io/udp/65535",
    "/dns4/example.io/udp/65535",
    "/dns6/example.io/udp/65535",
    "/dnsaddr/example.io/udp/65535",
  ]

  FailureVectors = [
    "/ip4",
    "/ip4/::1",
    "/ip4/fdpsofodsajfdoisa",
    "/ip6",
    "/ip6zone",
    "/ip6zone/",
    "/ip6zone//ip6/fe80::1",
    "/udp",
    "/tcp",
    "/sctp",
    "/udp/65536",
    "/tcp/65536",
    "/quic/65536",
    "/onion/9imaq4ygg2iegci7:80",
    "/onion/aaimaq4ygg2iegci7:80",
    "/onion/timaq4ygg2iegci7:0",
    "/onion/timaq4ygg2iegci7:-1",
    "/onion/timaq4ygg2iegci7",
    "/onion/timaq4ygg2iegci@:666",
    "/udp/1234/sctp",
    "/udp/1234/udt/1234",
    "/udp/1234/utp/1234",
    "/ip4/127.0.0.1/udp/jfodsajfidosajfoidsa",
    "/ip4/127.0.0.1/udp",
    "/ip4/127.0.0.1/tcp/jfodsajfidosajfoidsa",
    "/ip4/127.0.0.1/tcp",
    "/ip4/127.0.0.1/quic/1234",
    "/ip4/127.0.0.1/ipfs",
    "/ip4/127.0.0.1/ipfs/tcp",
    "/ip4/127.0.0.1/p2p",
    "/ip4/127.0.0.1/p2p/tcp",
    "/unix"
  ]

  RustSuccessVectors = [
    "/ip4/1.2.3.4",
    "/ip4/0.0.0.0",
    "/ip6/::1",
    "/ip6/2601:9:4f81:9700:803e:ca65:66e8:c21",
    "/udp/0",
    "/tcp/0",
    "/sctp/0",
    "/udp/1234",
    "/tcp/1234",
    "/sctp/1234",
    "/udp/65535",
    "/tcp/65535",
    "/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/udp/1234/sctp/1234",
    "/udp/1234/udt",
    "/udp/1234/utp",
    "/tcp/1234/http",
    "/tcp/1234/https",
    "/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/ip4/127.0.0.1/udp/1234",
    "/ip4/127.0.0.1/udp/0",
    "/ip4/127.0.0.1/tcp/1234",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/ip6/2001:8a0:7ac5:4201:3ac9:86ff:fe31:7095/tcp/8000/ws/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/p2p-webrtc-star/ip4/127.0.0.1/tcp/9090/ws/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/tcp/9090/p2p-circuit/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"
  ]

  RustSuccessExpects = [
    "0401020304",
    "0400000000",
    "2900000000000000000000000000000001",
    "29260100094F819700803ECA6566E80C21",
    "91020000",
    "060000",
    "84010000",
    "910204D2",
    "0604D2",
    "840104D2",
    "9102FFFF",
    "06FFFF",
    "A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
    "910204D2840104D2",
    "910204D2AD02",
    "910204D2AE02",
    "0604D2E003",
    "0604D2BB03",
    "A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B0604D2",
    "047F000001910204D2",
    "047F00000191020000",
    "047F0000010604D2",
    "047F000001A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
    "047F000001A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B0604D2",
    "29200108A07AC542013AC986FFFE317095061F40DD03A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
    "9302047F000001062382DD03A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B",
    "047F000001062382A202A503221220D52EBB89D85B02A284948203A62FF28389C57C9F42BEEC4EC20DB76A68911C0B"
  ]

  RustFailureVectors = [
    "/ip4",
    "/ip4/::1",
    "/ip4/fdpsofodsajfdoisa",
    "/ip6",
    "/udp",
    "/tcp",
    "/sctp",
    "/udp/65536",
    "/tcp/65536",
    "/onion/9imaq4ygg2iegci7:80",
    "/onion/aaimaq4ygg2iegci7:80",
    "/onion/timaq4ygg2iegci7:0",
    "/onion/timaq4ygg2iegci7:-1",
    "/onion/timaq4ygg2iegci7",
    "/onion/timaq4ygg2iegci@:666",
    "/udp/1234/sctp",
    "/udp/1234/udt/1234",
    "/udp/1234/utp/1234",
    "/ip4/127.0.0.1/udp/jfodsajfidosajfoidsa",
    "/ip4/127.0.0.1/udp",
    "/ip4/127.0.0.1/tcp/jfodsajfidosajfoidsa",
    "/ip4/127.0.0.1/tcp",
    "/ip4/127.0.0.1/ipfs",
    "/ip4/127.0.0.1/ipfs/tcp",
    "/p2p-circuit/50"
  ]

  PathVectors = [
    "/unix/tmp/p2pd.sock",
    "/unix/a/b/c/d/e/f/g/h/i.sock"
  ]

  PathExpects = [
    "90030E2F746D702F703270642E736F636B",
    "9003172F612F622F632F642F652F662F672F682F692E736F636B"
  ]

  UtilitySuccessVectors = [
    "/ip4/127.0.0.1/tcp/1024",
    "/ip4/127.0.0.1/udp/1024",
    "/ip4/0.0.0.0/tcp/1024",
    "/ip4/0.0.0.0/udp/1024",
    "/ip4/255.255.255.255/tcp/65535",
    "/ip4/255.255.255.255/udp/65535",
    "/ip6/::1/tcp/1024",
    "/ip6/::1/udp/1024",
    "/ip6/::/tcp/65535",
    "/ip6/::/udp/65535",
    "/ip6/::/udp/65535",
    "/dns/example.io/tcp/65535",
    "/dns4/example.io/tcp/65535",
    "/dns6/example.io/tcp/65535",
    "/dnsaddr/example.io/tcp/65535",
    "/unix/tmp/test.socket"
  ]

  UtilityFailVectors = [
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC/tcp/1234",
    "/ip6/2001:8a0:7ac5:4201:3ac9:86ff:fe31:7095/tcp/8000/ws/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/p2p-webrtc-star/ip4/127.0.0.1/tcp/9090/ws/ipfs/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC",
    "/ip4/127.0.0.1/udp/1234/quic"
  ]

  PatternVectors = [
    PatternVector(pattern: IP,
      good: @["/ip4/0.0.0.0", "/ip6/fc00::"],
      bad: @["/ip4/0.0.0.0/tcp/555", "/udp/789/ip6/fc00::"]
    ),
    PatternVector(pattern: TCP,
      good: @["/ip4/0.0.7.6/tcp/1234", "/ip6/::/tcp/0"],
      bad: @["/tcp/12345", "/ip6/fc00::/udp/5523/tcp/9543"]
    ),
    PatternVector(pattern: UDP,
      good: @["/ip4/0.0.7.6/udp/1234", "/ip6/::/udp/0"],
      bad: @["/udp/12345", "/ip6/fc00::/tcp/5523/udp/9543"],
    ),
    PatternVector(pattern: UTP,
      good: @["/ip4/1.2.3.4/udp/3456/utp", "/ip6/::/udp/0/utp"],
      bad: @[
        "/ip4/0.0.0.0/tcp/12345/utp",
        "/ip6/fc00::/ip4/0.0.0.0/udp/1234/utp",
        "/utp"
      ]
    ),
    PatternVector(pattern: QUIC,
      good: @["/ip4/1.2.3.4/udp/1234/quic", "/ip6/::/udp/1234/quic"],
      bad: @[
        "/ip4/0.0.0.0/tcp/12345/quic",
        "/ip6/fc00::/ip4/0.0.0.0/udp/1234/quic",
        "/quic"
      ]
    ),
    PatternVector(pattern: IPFS,
      good: @[
        "/ip4/1.2.3.4/tcp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip6/::/tcp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip6/::/udp/1234/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip4/0.0.0.0/udp/1234/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
      ],
      bad: @[
        "/ip4/1.2.3.4/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip6/::/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/tcp/123/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip6/::/udp/1234/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip6/::/utp/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
      ]
    ),
    PatternVector(pattern: DNS,
      good: @["/dns/example.io", "/dnsaddr/example.io", "/dns4/example.io", "/dns6/example.io"],
      bad: @["/ip4/127.0.0.1"],
    ),
    PatternVector(pattern: WebRTCDirect,
      good: @[
        "/ip4/1.2.3.4/tcp/3456/http/p2p-webrtc-direct",
        "/ip6/::/tcp/0/http/p2p-webrtc-direct"
      ],
      bad: @[
        "/ip4/0.0.0.0", "/ip6/fc00::", "/udp/12345",
        "/ip6/fc00::/tcp/5523/udp/9543"
      ]
    ),
    PatternVector(pattern: HTTP,
      good: @[
        "/ip4/1.2.3.4/http", "/dns4/example.io/http",
        "/dns6/::/tcp/7011/http", "/dnsaddr/example.io/http",
        "/ip6/fc00::/http"
      ],
      bad: @[
        "/ip4/1.2.3.4/https", "/ip4/0.0.0.0/tcp/12345/quic",
        "/ip6/fc00::/tcp/5523"
      ]
    ),
    PatternVector(pattern: HTTPS,
      good: @[
        "/ip4/1.2.3.4/https", "/dns4/example.io/https",
        "/dns6/::/tcp/7011/https", "/ip6/fc00::/https"
      ],
      bad: @[
        "/ip4/1.2.3.4/http", "/ip4/0.0.0.0/tcp/12345/quic",
        "/ip6/fc00::/tcp/5523"
      ]
    )
  ]

suite "MultiAddress test suite":

  test "go-multiaddr success test vectors":
    for item in SuccessVectors:
      var a = MultiAddress.init(item).get()
      check a.isEmpty() == false
      check a.validate() == true

  test "go-multiaddr failure test vectors":
    for item in FailureVectors:
      check MultiAddress.init(item).isErr()

  test "rust-multiaddr success test vectors":
    ## Rust test vectors are with changed UDP encoding and without WSS
    for i in 0..<len(RustSuccessVectors):
      var a = MultiAddress.init(RustSuccessVectors[i]).get()
      check:
        hex(a) == RustSuccessExpects[i]

  test "rust-multiaddr failure test vectors":
    for item in RustFailureVectors:
      var r = false
      try:
        discard MultiAddress.init(item).get()
      except:
        r = true
      check r == true

  test "Concatenation test":
    var ma1 = MultiAddress.init()
    var ma2 = MultiAddress.init()
    var ma3 = MultiAddress.init("/ip4/127.0.0.1").get()
    var ma4 = MultiAddress.init("/udp/30000").get()
    var ma5 = MultiAddress.init("/p2p-circuit").get()
    var cma = ma1 & ma3 & ma4 & ma5
    ma2 &= ma3
    ma2 &= ma4
    ma2 &= ma5
    check:
      $cma == "/ip4/127.0.0.1/udp/30000/p2p-circuit"
      $ma2 == "/ip4/127.0.0.1/udp/30000/p2p-circuit"

  test "isWire() test":
    for item in UtilitySuccessVectors:
      var a = MultiAddress.init(item).get()
      check a.isWire() == true
    for item in UtilityFailVectors:
      var a = MultiAddress.init(item).get()
      check a.isWire() == false

  test "Path addresses serialization/deserialization":
    for i in 0..<len(PathVectors):
      var a = MultiAddress.init(PathVectors[i]).get()
      check:
        hex(a) == PathExpects[i]
        $a == PathVectors[i]

  test "MultiAddress pattern matching test vectors":
    for item in PatternVectors:
      for gitem in item.good:
        var a = MultiAddress.init(gitem).get()
        check item.pattern.match(a) == true
      for bitem in item.bad:
        var a = MultiAddress.init(bitem).get()
        check item.pattern.match(a) == false

  test "MultiAddress init(\"tcp/udp/dccp/sctp\", int) test":
    check:
      $MultiAddress.init(multiCodec("tcp"), 0).get() == "/tcp/0"
      $MultiAddress.init(multiCodec("tcp"), 65535).get() == "/tcp/65535"
      $MultiAddress.init(multiCodec("tcp"), 34000).get() == "/tcp/34000"
      $MultiAddress.init(multiCodec("udp"), 0).get() == "/udp/0"
      $MultiAddress.init(multiCodec("udp"), 65535).get() == "/udp/65535"
      $MultiAddress.init(multiCodec("udp"), 34000).get() == "/udp/34000"
      $MultiAddress.init(multiCodec("dccp"), 0).get() == "/dccp/0"
      $MultiAddress.init(multiCodec("dccp"), 65535).get() == "/dccp/65535"
      $MultiAddress.init(multiCodec("dccp"), 34000).get() == "/dccp/34000"
      $MultiAddress.init(multiCodec("sctp"), 0).get() == "/sctp/0"
      $MultiAddress.init(multiCodec("sctp"), 65535).get() == "/sctp/65535"
      $MultiAddress.init(multiCodec("sctp"), 34000).get() == "/sctp/34000"

    check:
      MultiAddress.init(multiCodec("ip4"), 0).isErr()
      MultiAddress.init(multiCodec("ip6"), 0).isErr()
      MultiAddress.init(multiCodec("p2p"), 0).isErr()
      MultiAddress.init(multiCodec("tcp"), 65536).isErr()
      MultiAddress.init(multiCodec("udp"), 65536).isErr()
      MultiAddress.init(multiCodec("dccp"), 65536).isErr()
      MultiAddress.init(multiCodec("sctp"), 65536).isErr()
      MultiAddress.init(multiCodec("tcp"), -1).isErr()
      MultiAddress.init(multiCodec("udp"), -1).isErr()
      MultiAddress.init(multiCodec("dccp"), -1).isErr()
      MultiAddress.init(multiCodec("sctp"), -1).isErr()

  test "MultiAddress protoAddress(fixed) test":
    var
      address_v4: array[4, byte]
      address_v6: array[16, byte]
    check:
      MultiAddress.init("/ip4/0.0.0.0").get().protoAddress().get() == address_v4
      MultiAddress.init("/ip6/::0").get().protoAddress().get() == address_v6
