package com.github.ubiquitous.wheel

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import org.apache.log4j.Logger

import scala.collection.mutable

/**
  *
  * @author Namhwik on 2020-04-15 16:09
  */
class RingBufferWheel(val bfsize: Int, timeUnit: TimeUnit) extends Wheel {

  override val bufferSize: Int = bfsize

  val lock: ReentrantLock = new ReentrantLock

  val wheel: Array[mutable.Map[Int, mutable.Set[Task[Any]]]] =
    new Array[mutable.Map[Int, mutable.Set[Task[Any]]]](bufferSize)


  private val logger = Logger.getLogger(this.getClass)

  def addTask[T](task: Task[T]): Int = {
    logger.info(s"wheel : $timeUnit adding task : $task")

    val key = timeUnit match {
      case TimeUnit.SECONDS => task.seconds
      case TimeUnit.MINUTES => task.minutes
      case TimeUnit.HOURS => task.hours
    }

    try {
      lock.lock()
      task.cycle = cycleNum(key, bufferSize)
      if (mod(key, bufferSize) == tick.get() && task.cycle != 0)
        task.cycle -= 1
      val tasks: mutable.Map[Int, mutable.Set[Task[Any]]] = get(key)

      if (tasks != null) {
        tasks.get(task.cycle) match {
          case Some(taskSet) =>
            taskSet += task.asInstanceOf[Task[Any]]
          case None =>
            tasks.put(task.cycle, mutable.Set(task.asInstanceOf[Task[Any]]))
        }
      }
      else
        put(key, mutable.Map[Int, mutable.Set[Task[Any]]](task.cycle -> mutable.Set(task.asInstanceOf[Task[Any]])))
      logger.debug(s" current tick: ${tick.get} ,added :\n ${wheel.mkString("\n")}")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    finally
      lock.unlock()
    task.persist()
    key
  }

  //  def start(): Unit = {
  //    val trigger = new Trigger(this, this.timeUnit)
  //    logger.info(s"${this.timeUnit} trigger starting ...")
  //    Executor.addTrigger(trigger)
  //  }

  override def remove(index: Int): Set[Task[Any]] = {
    try {
      lock.lock()


      val tasks: mutable.Map[Int, mutable.Set[Task[Any]]] = wheel(index)
      if (tasks != null) {
        val ts1 = tasks.getOrElse(0, Set[Task[Any]]())
        wheel(index) = tasks.filter(_._1 > 0).map(x => (x._1 - 1, x._2))
        ts1.toSet
      } else null
    } finally lock.unlock()
  }


  override def get(key: Int): mutable.Map[Int, mutable.Set[Task[Any]]] = {
    wheel(mod(key, bufferSize))
  }

  override def put(key: Int, tasks: mutable.Map[Int, mutable.Set[Task[Any]]]): Unit = {
    wheel(mod(key, bufferSize)) = tasks
  }


  override def getTimeUnit: TimeUnit = timeUnit


}
