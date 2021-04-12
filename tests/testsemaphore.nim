import random
import chronos

import ../libp2p/utils/semaphore

import ./helpers
import ./asyncunit

randomize()

suite "AsyncSemaphore":
  asyncTest "should acquire":
    let sema = newAsyncSemaphore(3)

    await sema.acquire()
    await sema.acquire()
    await sema.acquire()

    check sema.count == 0

  asyncTest "should release":
    let sema = newAsyncSemaphore(3)

    await sema.acquire()
    await sema.acquire()
    await sema.acquire()

    check sema.count == 0
    sema.release()
    sema.release()
    sema.release()
    check sema.count == 3

  asyncTest "should queue acquire":
    let sema = newAsyncSemaphore(1)

    await sema.acquire()
    let fut = sema.acquire()

    check sema.count == -1
    sema.release()
    sema.release()
    check sema.count == 1

    await sleepAsync(10.millis)
    check fut.finished()

  asyncTest "should keep count == size":
    let sema = newAsyncSemaphore(1)
    sema.release()
    sema.release()
    sema.release()
    check sema.count == 1

  asyncTest "should tryAcquire":
    let sema = newAsyncSemaphore(1)
    await sema.acquire()
    check sema.tryAcquire() == false

  asyncTest "should tryAcquire and acquire":
    let sema = newAsyncSemaphore(4)
    check sema.tryAcquire() == true
    check sema.tryAcquire() == true
    check sema.tryAcquire() == true
    check sema.tryAcquire() == true
    check sema.count == 0

    let fut = sema.acquire()
    check fut.finished == false
    check sema.count == -1

    sema.release()
    sema.release()
    sema.release()
    sema.release()
    sema.release()

    check fut.finished == true
    check sema.count == 4

  asyncTest "should restrict resource access":
    let sema = newAsyncSemaphore(3)
    var resource = 0

    proc task() {.async.} =
      try:
        await sema.acquire()
        resource.inc()
        check resource > 0 and resource <= 3
        let sleep = rand(0..10).millis
        # echo sleep
        await sleepAsync(sleep)
      finally:
        resource.dec()
        sema.release()

    var tasks: seq[Future[void]]
    for i in 0..<10:
      tasks.add(task())

    await allFutures(tasks)

  asyncTest "should cancel sequential semaphore slot":
    let sema = newAsyncSemaphore(1)

    await sema.acquire()

    let tmp = sema.acquire()
    check not tmp.finished()

    tmp.cancel()
    sema.release()

    check await sema.acquire().withTimeout(10.millis)

  asyncTest "should handle out of order cancellations":
    let sema = newAsyncSemaphore(1)

    await sema.acquire()      # 1st acquire
    let tmp1 = sema.acquire() # 2nd acquire
    check not tmp1.finished()

    let tmp2 = sema.acquire() # 3rd acquire
    check not tmp2.finished()

    let tmp3 = sema.acquire() # 4th acquire
    check not tmp3.finished()

    # up to this point, we've called acquire 4 times
    tmp1.cancel() # 1st release (implicit)
    tmp2.cancel() # 2nd release (implicit)

    check not tmp3.finished() # check that we didn't release the wrong slot

    sema.release() # 3rd release (explicit)
    check tmp3.finished()

    sema.release() # 4th release
    check await sema.acquire().withTimeout(10.millis)

  asyncTest "should properly handle timeouts and cancellations":
    let sema = newAsyncSemaphore(1)

    await sema.acquire()
    check not(await sema.acquire().withTimeout(1.millis)) # should not acquire but cancel
    sema.release()

    check await sema.acquire().withTimeout(10.millis)
