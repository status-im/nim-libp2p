## Nim-LibP2P
## Copyright (c) 2021 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import std/[sugar, sets, sequtils, strutils]
import 
  chronos,
  chronicles,
  stew/[endians2, byteutils]
import ".."/[multiaddress, multicodec]

logScope:
  topics = "libp2p nameresolver"

type 
  NameResolver* = ref object of RootObj

method resolveTxt*(
  self: NameResolver,
  address: string): Future[seq[string]] {.async, base.} =
  ## Get TXT record
  ## 

  doAssert(false, "Not implemented!")

method resolveIp*(
  self: NameResolver,
  address: string,
  port: Port,
  domain: Domain = Domain.AF_UNSPEC): Future[seq[TransportAddress]] {.async, base.} =
  ## Resolve the specified address
  ## 

  doAssert(false, "Not implemented!")

proc getHostname(ma: MultiAddress): string =
  var dnsbuf = newSeq[byte](256)

  let dnsLen = ma[0].get().protoArgument(dnsbuf).get()
  dnsbuf.setLen(dnsLen)
  return string.fromBytes(dnsbuf)

proc resolveDnsAddress(
  self: NameResolver,
  ma: MultiAddress,
  domain: Domain = Domain.AF_UNSPEC,
  prefix = ""): Future[seq[MultiAddress]]
  {.async, raises: [Defect, MaError, TransportAddressError].} =
  #Resolve a single address
  var pbuf: array[2, byte]

  var dnsval = getHostname(ma)

  if ma[1].tryGet().protoArgument(pbuf).tryGet() == 0:
    raise newException(MaError, "Incorrect port number")
  let
    port = Port(fromBytesBE(uint16, pbuf))
    resolvedAddresses = await self.resolveIp(prefix & dnsval, port, domain)
 
  var addressSuffix = ma
  return collect(newSeqOfCap(4)):
    for address in resolvedAddresses:
      var createdAddress = MultiAddress.init(address).tryGet()[0].tryGet()
      for part in ma:
        if DNS.match(part.get()): continue
        createdAddress &= part.tryGet()
      createdAddress

func matchDnsSuffix(m1, m2: MultiAddress): MaResult[bool] =
  for partMaybe in m1:
    let part = ?partMaybe
    if DNS.match(part): continue
    let entryProt = ?m2[?part.protoCode()]
    if entryProt != part:
      return ok(false)
  return ok(true)

proc resolveDnsAddr(
  self: NameResolver,
  ma: MultiAddress,
  depth: int = 0): Future[seq[MultiAddress]]
  {.async.} =

  trace "Resolving dnsaddr", ma
  if depth > 6:
    info "Stopping DNSADDR recursion, probably malicious", ma
    return @[]

  var dnsval = getHostname(ma)

  let txt = await self.resolveTxt("_dnsaddr." & dnsval)

  trace "txt entries", txt

  var result: seq[MultiAddress]
  for entry in txt:
    if not entry.startsWith("dnsaddr="): continue
    let entryValue = MultiAddress.init(entry[8..^1]).tryGet()

    if not matchDnsSuffix(ma, entryValue).tryGet(): continue

    # The spec is not clear wheter only DNSADDR can be recursived
    # or any DNS addr. Only handling DNSADDR because it's simpler
    # to avoid infinite recursion
    if DNSADDR.matchPartial(entryValue):
      let resolved = await self.resolveDnsAddr(entryValue, depth + 1)
      for r in resolved:
        result.add(r)
    else:
      result.add(entryValue)

  if result.len == 0:
    debug "Failed to resolve any DNSADDR", ma
    return @[ma]
  return result


proc resolveMAddresses*(
  self: NameResolver,
  addrs: seq[MultiAddress]): Future[seq[MultiAddress]] {.async.} =
  var res = initOrderedSet[MultiAddress]()

  for address in addrs:
    if not DNS.matchPartial(address):
      res.incl(address)
    else:
      let code = address[0].get().protoCode().get()
      let seq = case code:
        of multiCodec("dns"):
          await self.resolveDnsAddress(address)
        of multiCodec("dns4"):
          await self.resolveDnsAddress(address, Domain.AF_INET)
        of multiCodec("dns6"):
          await self.resolveDnsAddress(address, Domain.AF_INET6)
        of multiCodec("dnsaddr"):
          await self.resolveDnsAddr(address)
        else:
          @[address]
      for ad in seq:
        res.incl(ad)
  return res.toSeq
