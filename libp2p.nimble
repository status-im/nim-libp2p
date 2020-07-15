mode = ScriptMode.Verbose

packageName   = "libp2p"
version       = "0.0.2"
author        = "Status Research & Development GmbH"
description   = "LibP2P implementation"
license       = "MIT"
skipDirs      = @["tests", "examples", "Nim"]

requires "nim >= 1.2.0",
         "nimcrypto >= 0.4.1",
         "bearssl >= 0.1.4",
         "chronicles >= 0.7.2",
         "chronos >= 2.3.8",
         "metrics",
         "secp256k1",
         "stew >= 0.1.0"

proc runTest(filename: string, verify: bool = true, sign: bool = true,
             moreoptions: string = "") =
  var excstr = "nim c --opt:speed -d:debug --verbosity:0 --hints:off"
  excstr.add(" --warning[CaseTransition]:off --warning[ObservableStores]:off --warning[LockLevel]:off")
  excstr.add(" -d:libp2p_pubsub_sign=" & $sign)
  excstr.add(" -d:libp2p_pubsub_verify=" & $verify)
  excstr.add(" " & moreoptions & " ")
  if verify and sign:
    # build it with TRACE and JSON logs
    exec excstr & " -d:chronicles_log_level=TRACE -d:chronicles_sinks:json" & " tests/" & filename
  # build it again, to run it with less verbose logs
  exec excstr & " -d:chronicles_log_level=INFO -r" & " tests/" & filename
  rmFile "tests/" & filename.toExe

proc buildSample(filename: string) =
  var excstr = "nim c --opt:speed --threads:on -d:debug --verbosity:0 --hints:off"
  excstr.add(" --warning[CaseTransition]:off --warning[ObservableStores]:off --warning[LockLevel]:off")
  excstr.add(" examples/" & filename)
  exec excstr
  rmFile "examples" & filename.toExe

task testnative, "Runs libp2p native tests":
  runTest("testnative")

task testdaemon, "Runs daemon tests":
  runTest("testdaemon")

task testinterop, "Runs interop tests":
  runTest("testinterop")

task testpubsub, "Runs pubsub tests":
  runTest("pubsub/testpubsub")
  runTest("pubsub/testpubsub", sign = false, verify = false)

task testfilter, "Run PKI filter test":
  runTest("testpkifilter",
           moreoptions = "-d:libp2p_pki_schemes=\"secp256k1\"")
  runTest("testpkifilter",
           moreoptions = "-d:libp2p_pki_schemes=\"secp256k1;ed25519\"")
  runTest("testpkifilter",
           moreoptions = "-d:libp2p_pki_schemes=\"secp256k1;ed25519;ecnist\"")
  runTest("testpkifilter",
           moreoptions = "-d:libp2p_pki_schemes=")

task test, "Runs the test suite":
  exec "nimble testnative"
  exec "nimble testpubsub"
  exec "nimble testdaemon"
  exec "nimble testinterop"
  exec "nimble testfilter"

task examples_build, "Build the samples":
  buildSample("directchat")
