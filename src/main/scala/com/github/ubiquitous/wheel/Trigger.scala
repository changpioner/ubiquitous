package com.github.ubiquitous.wheel

import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/**
  *
  * @author Namhwik on 2020-04-15 15:49
  */
class Trigger(wheel: Wheel, timeUnit: TimeUnit) extends Runnable {

  private val tick = wheel.tick

  override def run(): Unit = {

    while (!wheel.stop) {
      try {
        val index = tick.get()
        if (index + 1 > wheel.bufferSize - 1) {
          tick.set(0)
        }
        val task = wheel.remove(index)
        if (task != null && task.nonEmpty) {
          timeUnit match {
            case TimeUnit.SECONDS =>
              task.foreach(Executor.submit)
            case TimeUnit.MINUTES =>
              task.map(t => t.setSpan(t.seconds)).foreach(WheelFactory.unitWheel(TimeUnit.SECONDS).get.addTask)
            case TimeUnit.HOURS =>
              task.map(t => t.setSpan(t.minutes)).foreach(WheelFactory.unitWheel(TimeUnit.MINUTES).get.addTask)
          }
        }
        //Total tick number of records
        tick.incrementAndGet()

        timeUnit.sleep(1)

      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
    println("delay task is stopped")

  }


}
