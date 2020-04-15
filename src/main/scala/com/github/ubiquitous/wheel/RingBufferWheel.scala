package com.github.ubiquitous.wheel

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger

import com.github.ubiquitous.wheel.Task

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

/**
  *
  * @author Namhwik on 2020-04-15 16:09
  */
class RingBufferWheel(
                       val executorService: ExecutorService,
                       val bufferSize: Int
                     ) {
  val wheel: Array[mutable.Set[Task]] = new Array[mutable.Set[Task]](bufferSize)
  var stop: Boolean = false
  val tick = new AtomicInteger(0)

  private val lock: ReentrantLock = new ReentrantLock


  def addTask(task: Task): Int = {
    val key = task.span
    task.cycle = cycleNum(key, bufferSize)

    try {
      lock.lock()
      val tasks = get(key)
      if (tasks != null)
        tasks += task
      else
        put(key, mutable.Set(task))
    } finally lock.unlock()
    key
  }

  def start(): Unit = {
    val trigger = new Thread(new Trigger(this))
    trigger.setName("buffer trigger thread")
    trigger.start()
  }

  def remove(index: Int): Set[Task] = {
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

  private def cycleNum(target: Int, mod: Int) = { //equals target/mod
    target >> Integer.bitCount(mod - 1)
  }

  private def get(key: Int): mutable.Set[Task] = {
    val index = mod(key, bufferSize)
    wheel(index)
  }

  private def put(key: Int, tasks: mutable.Set[Task]): Unit = {
    val index = mod(key, bufferSize)
    wheel(index) = tasks
  }

  private def mod(target: Int, mod: Int) = { // equals target % mod
    (target + tick.get) & (mod - 1)
  }

}
