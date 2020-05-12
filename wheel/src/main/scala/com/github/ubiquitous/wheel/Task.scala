package com.github.ubiquitous.wheel

import java.util.concurrent.{Callable, TimeUnit}

import com.github.ubiquitous.config.Conf.TIME_UNIT

/**
  *
  * @author Namhwik on 2020-04-15 16:12
  */
abstract class Task[T](val dl: Int) extends Callable[T] {

  var span: Int = dl

  var cycle: Int = _

  val (seconds, minutes, hours) = TIME_UNIT match {
    case TimeUnit.SECONDS =>
      (dl % 60, (dl / 60) % 60, dl / 3600)
    case TimeUnit.MINUTES =>
      (0, dl % 60, (dl / 60) % 60)
    case TimeUnit.HOURS =>
      (0, 0, dl % 60)
  }

  def setSpan(sp: Int): Task[T] = {
    span = sp
    this
  }

  override def toString: String = s"cycle : $cycle,dl :$dl ,span : $span , seconds : $seconds ,minutes :$minutes , hours : $hours"


}
