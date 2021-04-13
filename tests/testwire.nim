import pkg/asynctest
import pkg/chronos
import ../libp2p/wire
import ../libp2p/multiaddress

suite "initTAddress":

  test "initializes ipv4 udp address":
    let multi = MultiAddress.init("/ip4/127.0.0.1/udp/45894").get()
    check initTAddress(multi).get == initTAddress("127.0.0.1:45894")

  test "initializes ipv4 tcp address":
    let multi = MultiAddress.init("/ip4/127.0.0.1/tcp/42587").get()
    check initTAddress(multi).get == initTAddress("127.0.0.1:42587")

  test "initializes ipv6 udp address":
    let multi = MultiAddress.init("/ip6/::1/udp/45894").get()
    check initTAddress(multi).get == initTAddress("::1:45894")

  test "initializes ipv6 tcp address":
    let multi = MultiAddress.init("/ip6/::1/tcp/42587").get()
    check initTAddress(multi).get == initTAddress("::1:42587")

  test "initializes ipv4 quic address":
    let multi = MultiAddress.init("/ip4/127.0.0.1/udp/45894/quic").get()
    check initTAddress(multi).get == initTAddress("127.0.0.1:45894")

  test "initializes ipv6 quic address":
    let multi = MultiAddress.init("/ip6/::1/udp/45894/quic").get()
    check initTAddress(multi).get == initTAddress("::1:45894")
