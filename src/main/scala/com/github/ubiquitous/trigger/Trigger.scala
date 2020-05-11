package com.github.ubiquitous.trigger

import java.util.concurrent.TimeUnit

import com.github.ubiquitous.exe.Executor
import com.github.ubiquitous.wheel.{Wheel, WheelFactory}
import org.apache.log4j.Logger


/**
  *
  * @author Namhwik on 2020-04-15 15:49
  */
class Trigger(wheel: Wheel, timeUnit: TimeUnit) extends Runnable {

  private val tick = wheel.tick
  private val logger = Logger.getLogger(this.getClass)

  override def run(): Unit = {

    while (!wheel.stop) {
      try {

        val index = if (tick.compareAndSet(wheel.bufferSize - 1, 0)) {
          0
        } else
        //Total tick number of records
          tick.incrementAndGet()
        val task = wheel.remove(index)
        logger.debug(s"current running index :$index")
        logger.debug(s"Got tasks : ${if (task == null) "null" else task.mkString(" , ")}")
        if (task != null && task.nonEmpty) {
          timeUnit match {
            case TimeUnit.SECONDS =>
              logger.debug("submit task ...")
              task.foreach(t => Executor.submit(t))
            case TimeUnit.MINUTES =>
              logger.debug("moving task to seconds wheel ...")
              val (nextWheelTasks, toRunTasks) = task.map(t => t.setSpan(t.seconds)).partition(_.span > 0)
              toRunTasks.foreach(t => Executor.submit(t))
              nextWheelTasks.foreach(task => {
                WheelFactory.unitWheel(TimeUnit.SECONDS).get.addTask(task)
              })
            case TimeUnit.HOURS =>
              logger.debug("moving task to minutes wheel ...")
              task.map(t => t.setSpan(t.minutes)).foreach(WheelFactory.unitWheel(TimeUnit.MINUTES).get.addTask)
          }
        }

        //Logger.getLogger(this.getClass).info("go to sleep ...")
        timeUnit.sleep(1)


      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
    logger.info(s"Task trigger : $timeUnit is stopped !")

  }


}
