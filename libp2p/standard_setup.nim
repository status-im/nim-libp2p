## Nim-LibP2P
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

import
  options, tables
import libp2p

export libp2p

proc newStandardSwitch*(privKey = none(PrivateKey),
                        address = MultiAddress.init("/ip4/127.0.0.1/tcp/0"),
                        triggerSelf = false,
                        gossip = false): Switch =
  proc createMplex(conn: Connection): Muxer =
    result = newMplex(conn)

  let
    seckey = privKey.get(otherwise = PrivateKey.random(ECDSA))
    peerInfo = PeerInfo.init(seckey, [address])
    mplexProvider = newMuxerProvider(createMplex, MplexCodec)
    transports = @[Transport(newTransport(TcpTransport))]
    muxers = {MplexCodec: mplexProvider}.toTable
    identify = newIdentify(peerInfo)
    secureManagers = {SecioCodec: Secure(newSecio seckey)}.toTable
    pubSub = if gossip: PubSub newPubSub(GossipSub, peerInfo, triggerSelf)
             else: PubSub newPubSub(FloodSub, peerInfo, triggerSelf)

  result = newSwitch(peerInfo,
                     transports,
                     identify,
                     muxers,
                     secureManagers = secureManagers,
                     pubSub = some(pubSub))
