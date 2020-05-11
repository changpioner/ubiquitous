package com.github.ubiquitous.wheel

import java.util.concurrent.Callable

/**
  *
  * @author Namhwik on 2020-04-15 16:12
  */
abstract class Task[T](val dl: Int) extends Callable[T] {

  var span: Int = dl

  var cycle: Int = _

  val seconds: Int = dl % 60

  val minutes: Int = (dl / 60) % 60

  val hours: Int = dl / 3600

  def setSpan(sp: Int): Task[T] = {
    span = sp
    this
  }

  override def toString: String = s"cycle : $cycle,dl :$dl ,span : $span , seconds : $seconds ,minutes :$minutes , hours : $hours"


}
