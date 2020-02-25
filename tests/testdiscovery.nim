import unittest, tables
import chronos

import ../libp2p/[protocols/discovery/discovery,
                 protocols/discovery/bootstrap,
                 switch,
                 connection,
                 peerinfo]

suite "Discovery":
  suite "Bootstrap":
    test "should get peers":
      proc test(): Future[bool] {.async.} =

      check:
        waitFor(test()) == true
