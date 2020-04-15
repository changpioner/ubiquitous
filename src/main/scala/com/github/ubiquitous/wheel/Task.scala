package com.github.ubiquitous.wheel

/**
  *
  * @author Namhwik on 2020-04-15 16:12
  */
abstract class Task(val span: Int) extends Thread {
  var cycle: Int = _

  def setCycle(c: Int): Unit = {
    cycle = c
  }
}
