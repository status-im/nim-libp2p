import os, osproc, streams, strutils
import parseutils

proc toExe*(filename: string): string =
  (when defined(windows): filename & ".exe" else: filename)

const
  nimFlags = "--hints:off --warning[ObservableStores]:off --warning[LockLevel]:off"
  nimCommand = "nim c " & nimFlags & " --path:../ "

let markdownFile = paramStr(1)

let contents = markdownFile.readFile
var index = 0
var codes: seq[string]

const startDelim = "```nim\n"
const endDelim = "\n```"
while true:
  let startOfBlock = contents.find(startDelim, start = index)
  if startOfBlock == -1: break

  let endOfBlock = contents.find(endDelim, start = startOfBlock + startDelim.len)
  if endOfBlock == -1:
    quit "Unfinished block!"

  let code = contents[startOfBlock + startDelim.len .. endOfBlock]
  if code.startsWith("import"): codes.add(code)
  else: codes[^1] &= code

  index = endOfBlock + endDelim.len

for index, code in codes:
  let filename = markdownFile.changeFileExt("") & "_" & $index & "md.nim"
  filename.writeFile(code)
  var res = execCmdEx(nimCommand & filename)
  echo res[0]
  if res.exitCode != 0:
    quit "Failed to compile"
  removeFile "tests/" & filename.changeFileExt("").toExe
