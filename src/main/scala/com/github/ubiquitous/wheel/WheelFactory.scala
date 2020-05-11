package com.github.ubiquitous.wheel

import java.util.concurrent.TimeUnit

/**
  *
  * @author Namhwik on 2020-04-28 18:01
  */
object WheelFactory {

  val DEFAULT_BUFFER_SIZE = 8

  private val wheels: Map[TimeUnit, RingBufferWheel] = List(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS).map(build).toMap

  private def build(timeUnit: TimeUnit): (TimeUnit, RingBufferWheel) = {
    val ringBufferWheel = new RingBufferWheel(DEFAULT_BUFFER_SIZE, timeUnit)
    ringBufferWheel.start()
    (timeUnit, ringBufferWheel)
  }

  def unitWheel(timeUnit: TimeUnit): Option[RingBufferWheel] = synchronized {
    wheels.get(timeUnit)
  }

  def addDelayTask[T](task: Task[T]): Unit = {

    task.hours match {
      case 0 =>
        task.minutes match {
          case 0 =>
            unitWheel(TimeUnit.SECONDS).get.addTask[T](task.setSpan(task.seconds))
          case _ =>
            unitWheel(TimeUnit.MINUTES).get.addTask[T](task.setSpan(task.minutes))
        }
      case _ =>
        unitWheel(TimeUnit.HOURS).get.addTask[T](task.setSpan(task.hours))
    }

  }

  def close(): Unit = {

  }

}
