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


  val wheel = new Array[mutable.Set[Task[Any]]](bufferSize)


  private val logger = Logger.getLogger(this.getClass)

  def addTask[T](task: Task[T]): Int = {
    logger.info(s"wheel : $timeUnit adding task : $task")
    val key = task.span

    try {
      lock.lock()
      task.cycle = cycleNum(key, bufferSize)
      if (mod(key, bufferSize) == tick.get())
        task.cycle -= 1
      val tasks: mutable.Set[Task[Any]] = get(key)
      if (tasks != null)
        tasks += task.asInstanceOf[Task[Any]]
      else
        put(key, mutable.Set[Task[Any]](task.asInstanceOf[Task[Any]]))
      logger.debug(s" current tick: ${tick.get} ,added :\n ${wheel.mkString("\n")}")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }

    finally lock.unlock()

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
      val tasks = wheel(index)
      if (tasks != null) {
        val (ts1, ts2) = tasks.partition(_.cycle == 0)
        ts1.foreach(tasks.remove)
        ts2.foreach(_.cycle -= 1)
        ts1.toSet
      } else null
    } finally lock.unlock()
  }


  override def get(key: Int): mutable.Set[Task[Any]] = {
    val index = mod(key, bufferSize)
    wheel(index)
  }

  override def put(key: Int, tasks: mutable.Set[Task[Any]]): Unit = {
    val index = mod(key, bufferSize)
    wheel(index) = tasks
  }

  override def getTimeUnit: TimeUnit = timeUnit
}
