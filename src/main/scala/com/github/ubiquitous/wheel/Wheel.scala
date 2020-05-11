package com.github.ubiquitous.wheel

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
  *
  * @author Namhwik on 2020-04-28 16:58
  */
trait Wheel {
  val bufferSize: Int
  val tick = new AtomicInteger(0)

  var stop: Boolean = false

  def cycleNum(target: Int, mod: Int): Int = { //equals target/mod
    target >> Integer.bitCount(mod - 1)
  }

  def get(key: Int): mutable.Set[Task[Any]]

  def put(key: Int, tasks: mutable.Set[Task[Any]])

  def remove(index: Int): Set[Task[Any]]

  def mod(target: Int, mod: Int): Int = { // equals target % mod
    (target + tick.get) & (mod - 1)
  }

}
