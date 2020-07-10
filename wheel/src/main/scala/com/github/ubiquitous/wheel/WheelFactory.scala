package com.github.ubiquitous.wheel

import java.util.concurrent.TimeUnit

import com.github.ubiquitous.config.Conf
import com.github.ubiquitous.config.Conf.TIME_UNIT
import com.github.ubiquitous.exe.Executor
import com.github.ubiquitous.exe.Executor.TRIGGER_CORE_SIZE
import com.github.ubiquitous.trigger.{MultipleTrigger, Trigger}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-04-28 18:01
  */
object WheelFactory {

  private val logger: Logger = Logger.getLogger(this.getClass)
  lazy private val trigger = new Trigger(TIME_UNIT)

  val DEFAULT_BUFFER_SIZE = 8

  val tuList: List[TimeUnit] = List(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS)
    .filter(tu => tu.compareTo(TIME_UNIT) >= 0)

  private lazy val wheels: Map[TimeUnit, RingBufferWheel] =
    tuList.map(build).toMap

  private def build(timeUnit: TimeUnit): (TimeUnit, RingBufferWheel) = {
    val ringBufferWheel = new RingBufferWheel(DEFAULT_BUFFER_SIZE, timeUnit)
    //ringBufferWheel.start()
    (timeUnit, ringBufferWheel)
  }

  def unitWheel(timeUnit: TimeUnit): Option[RingBufferWheel] = synchronized {
    wheels.get(timeUnit)
  }

  def addDelayTask[T](task: Task[T]): Unit = {

    task.hours match {
      case 0 =>
        if (TIME_UNIT.compareTo(TimeUnit.MINUTES) <= 0)
          task.minutes match {
            case 0 =>
              if (TIME_UNIT.compareTo(TimeUnit.SECONDS) <= 0) {
                if (task.seconds == 0)
                  Executor.submit(task)
                else
                  unitWheel(TimeUnit.SECONDS).get.addTask[T](task)
              }
              else
                Executor.submit(task)
            case _ =>
              unitWheel(TimeUnit.MINUTES).get.addTask[T](task)
          }
        else {
          Executor.submit(task)
        }
      case _ =>
        unitWheel(TimeUnit.HOURS).get.addTask[T](task)
    }
    task.createTrigger = (trigger.triggerMin, trigger.triggerSec)
  }

  def start(): Unit = {

    Conf.getString("triggerMod") match {
      case "single" =>
        logger.info(s"$TIME_UNIT trigger starting ...")
        Executor.addTrigger(trigger)
      case "multiple" =>
        tuList.map(t => new MultipleTrigger(WheelFactory.unitWheel(t).get, t)).foreach(Executor.addTrigger)
      case _ =>
        throw new IllegalArgumentException(s"trigger mod does not support ${Conf.getString("triggerMod")}")
    }


  }

  def close(): Unit = {

  }

  start()

  def printTasks(): Unit = {
    wheels.foreach {
      x =>
        try {
          x._2.wheel.foreach(_.foreach(_._2.foreach(println)))
        } catch {
          case ex: Exception =>

        }
    }
  }
}
