package com.github.ubiquitous.wheel

/**
  *
  * @author Namhwik on 2020-04-15 16:12
  */
abstract class Task(var dl: Int) extends Thread {

  var span: Int = dl

  var cycle: Int = _

  val seconds: Int = dl % 60

  val minutes: Int = (dl / 60) % 60

  val hours: Int = dl / 3600

  def setSpan(sp: Int): Task = {
    span = dl
    this
  }

}
