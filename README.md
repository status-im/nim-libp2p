<h1 align="center">
  <a href="https://libp2p.io"><img width="250" src="https://github.com/libp2p/libp2p/blob/master/logo/black-bg-2.png?raw=true" alt="libp2p hex logo" /></a>
</h1>

<h3 align="center">The Nim implementation of the libp2p Networking Stack.</h3>

<p align="center">
<a href="https://travis-ci.org/status-im/nim-libp2p"><img src="https://travis-ci.org/status-im/nim-libp2p.svg?branch=master" /></a>
<a href="https://ci.appveyor.com/project/nimbus/nim-libp2p/branch/master"><img src="https://ci.appveyor.com/api/projects/status/pqgif5bcie6cp3wi/branch/master?svg=true" /></a>
<a href="https://dev.azure.com/nimbus-dev/nim-libp2p/_build?definitionId=5&branchName=master"><img src="https://img.shields.io/azure-devops/build/nimbus-dev/dc5eed24-3f6c-4c06-8466-3d060abd6c8b/5/master?label=Azure%20%28Linux%2064-bit%2C%20Windows%2032-bit%2F64-bit%2C%20MacOS%2064-bit%29" /></a>
</p>

<p align="center">
<a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" /></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" /></a>
<img src="https://img.shields.io/badge/nim-%3E%3D1.2.0-orange.svg?style=flat-square" />
</p>

## Introduction

An implementation of [libp2p](https://libp2p.io/) in Nim.

## Project Status
libp2p is now used in production by a few projects at [status](https://github.com/status-im), including [nimbus](https://github.com/status-im/nimbus-eth2).

While far from complete, currently available componets are fairly stable.

Check our [examples folder](/examples) to get started!

# Table of Contents
- [Background](#background)
- [Install](#install)
  - [Prerequisite](#prerequisite)
- [Usage](#usage)
  - [API](#api)
  - [Getting Started](#getting-started)
  - [Tutorials and Examples](#tutorials-and-examples)
  - [Using the Go Daemon](#using-the-go-daemon)
- [Development](#development)
  - [Tests](#tests)
  - [Packages](#packages)
- [Contribute](#contribute)
  - [Core Developers](#core-developers)
- [License](#license)

## Background
libp2p is a networking stack and library modularized out of [The IPFS Project](https://github.com/ipfs/ipfs), and bundled separately for other tools to use.

libp2p is the product of a long and arduous quest of understanding; a deep dive into the internet's network stack and the peer-to-peer protocols from the past. Building large scale peer-to-peer systems has been complex and difficult in the last 15 years and libp2p is a way to fix that. It is a "network stack", a suite of networking protocols that cleanly separates concerns and enables sophisticated applications to only use the protocols they absolutely need, without giving up interoperability and upgradeability.

libp2p grew out of IPFS, but it is built so that lots of people can use it, for lots of different projects.

- Learn more about libp2p at [**libp2p.io**](https://libp2p.io) and follow our evolving documentation efforts at [**docs.libp2p.io**](https://docs.libp2p.io).
- [Here](https://github.com/libp2p/libp2p#description) is an overview of libp2p and its implementations in other programming languages.

## Install
```
nimble install libp2p
```
### Prerequisite
- [Nim](https://nim-lang.org/install.html)

## Usage

### API
The specification is available in the [docs/api](docs/api) folder.

### Getting Started
Please read the [GETTING_STARTED.md](docs/GETTING_STARTED.md) guide.

### Tutorials and Examples
Example code can be found in the [examples folder](/examples).

#### Direct Chat Tutorial
- [Part I](https://our.status.im/nim-libp2p-tutorial-a-peer-to-peer-chat-example-1/): Set up the main function and use multi-thread for processing IO.
- [Part II](https://our.status.im/nim-libp2p-tutorial-a-peer-to-peer-chat-example-2/): Dial remote peer and allow customized user input commands.
- [Part III](https://our.status.im/nim-libp2p-tutorial-a-peer-to-peer-chat-example-3/): Configure and establish a libp2p node.


### Using the Go Daemon
Please find the installation and usage intructions in [daemonapi.md](docs/api/libp2p/daemonapi.md).

Examples can be found in the [examples/go-daemon folder](https://github.com/status-im/nim-libp2p/tree/readme/examples/go-daemon);

## Development
**Clone and Install dependencies:**

```sh
git clone https://github.com/status-im/nim-libp2p
cd nim-libp2p
nimble install
```
#### Run unit tests
```sh
# run all the unit tests
nimble test
```

### Packages

List of packages currently in existence for nim-libp2p:

#### Libp2p
- [libp2p](https://github.com/status-im/nim-libp2p)
- [libp2p-daemon-client](https://github.com/status-im/nim-libp2p/blob/master/libp2p/daemon/daemonapi.nim)
- [interop-libp2p](https://github.com/status-im/nim-libp2p/blob/master/tests/testinterop.nim)

#### Transports
- [libp2p-tcp](https://github.com/status-im/nim-libp2p/blob/master/libp2p/transports/tcptransport.nim)

#### Secure Channels
- [libp2p-secio](https://github.com/status-im/nim-libp2p/blob/master/libp2p/protocols/secure/secio.nim)

#### Stream Multiplexers
- [libp2p-mplex](https://github.com/status-im/nim-libp2p/blob/master/libp2p/muxers/mplex/mplex.nim)

#### Utilities
- [libp2p-crypto](https://github.com/status-im/nim-libp2p/tree/master/libp2p/crypto)
- [libp2p-crypto-secp256k1](https://github.com/status-im/nim-libp2p/blob/master/libp2p/crypto/secp.nim)

#### Data Types
- [peer-id](https://github.com/status-im/nim-libp2p/blob/master/libp2p/peer.nim)
- [peer-info](https://github.com/status-im/nim-libp2p/blob/master/libp2p/peerinfo.nim)

#### Pubsub
- [libp2p-pubsub](https://github.com/status-im/nim-libp2p/blob/master/libp2p/protocols/pubsub/pubsub.nim)
- [libp2p-floodsub](https://github.com/status-im/nim-libp2p/blob/master/libp2p/protocols/pubsub/floodsub.nim)
- [libp2p-gossipsub](https://github.com/status-im/nim-libp2p/blob/master/libp2p/protocols/pubsub/gossipsub.nim)


Packages that exist in the original libp2p specs and are under active development:
- libp2p-daemon
- libp2p-webrtc-direct
- libp2p-webrtc-star
- libp2p-websockets
- libp2p-spdy
- libp2p-bootstrap
- libp2p-kad-dht
- libp2p-mdns
- libp2p-webrtc-star
- libp2p-delegated-content-routing
- libp2p-delegated-peer-routing
- libp2p-nat-mgnr
- libp2p-utils

** Note that the current stack reflects the minimal requirements for the upcoming Eth2 implementation.

### Tips and tricks

#### enable expensive metrics:

```bash
nim c -d:libp2p_expensive_metrics some_file.nim
```

#### use identify metrics

```bash
nim c -d:libp2p_agents_metrics -d:KnownLibP2PAgents=nimbus,lighthouse,prysm,teku some_file.nim
```

### specify gossipsub specific topics to measure

```bash
nim c -d:KnownLibP2PTopics=topic1,topic2,topic3 some_file.nim
```

## Contribute
The libp2p implementation in Nim is a work in progress. We welcome contributors to help out! Specifically, you can:
- Go through the modules and **check out existing issues**. This would be especially useful for modules in active development. Some knowledge of IPFS/libp2p may be required, as well as the infrastructure behind it.
- **Perform code reviews**. Feel free to let us know if you found anything that can a) speed up the project development b) ensure better quality and c) reduce possible future bugs.
- **Add tests**. Help nim-libp2p to be more robust by adding more tests to the [tests folder](https://github.com/status-im/nim-libp2p/tree/master/tests).

### Core Developers
[@cheatfate](https://github.com/cheatfate), [Dmitriy Ryajov](https://github.com/dryajov), [Giovanni Petrantoni](https://github.com/sinkingsugar), [Zahary Karadjov](https://github.com/zah)

## License

Licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. These files may not be copied, modified, or distributed except according to those terms.

