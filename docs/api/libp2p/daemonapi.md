# Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
  - [Example](#example)
  - [Getting Started](#getting-started)

# Introduction
This is a libp2p-backed daemon wrapping the functionalities of go-libp2p for use in Nim. <br>
For more information about the go daemon, check out [this repository](https://github.com/libp2p/go-libp2p-daemon).

# Installation
```sh
# clone and install dependencies
git clone https://github.com/status-im/nim-libp2p
cd nim-libp2p
nimble install

# perform unit tests
nimble test

# update the git submodule to install the go daemon 
git submodule update --init --recursive
go version
git clone https://github.com/libp2p/go-libp2p-daemon
cd go-libp2p-daemon
git checkout v0.0.1
go install ./...
cd ..
```

# Usage 

## Example
Examples can be found in the [examples folder](https://github.com/status-im/nim-libp2p/tree/readme/examples/go-daemon)

## Getting Started
Try out the chat example. Full code can be found [here](https://github.com/status-im/nim-libp2p/blob/master/examples/chat.nim):

```bash
nim c -r --threads:on examples/directchat.nim
```

This will output a peer ID such as `QmbmHfVvouKammmQDJck4hz33WvVktNEe7pasxz2HgseRu` which you can use in another instance to connect to it.

```bash
./examples/directchat
/connect QmbmHfVvouKammmQDJck4hz33WvVktNEe7pasxz2HgseRu
```

You can now chat between the instances!

![Chat example](https://imgur.com/caYRu8K.gif)



