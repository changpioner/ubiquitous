package com.github.ubiquitous.wheel

import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/**
  *
  * @author Namhwik on 2020-04-15 15:49
  */
class Trigger(wheel: RingBufferWheel) extends Runnable {

  private val tick = wheel.tick

  override def run(): Unit = {

    while (!wheel.stop) {
      try {
        val index = tick.get()
        if (index + 1 > wheel.bufferSize - 1) {
          tick.set(0)
        }
        val task = wheel.remove(index)
        if (task != null && task.nonEmpty)
          task.foreach(wheel.executorService.submit)
        //Total tick number of records
        tick.incrementAndGet()
        TimeUnit.SECONDS.sleep(1)

      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
    println("delay task is stopped")

  }


}
