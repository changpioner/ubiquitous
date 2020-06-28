package com.github.ubiquitous.wheel

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import java.util.concurrent.{Callable, TimeUnit}

import com.github.ubiquitous.config.Conf.TIME_UNIT

/**
  *
  * @author Namhwik on 2020-04-15 16:12
  */
trait Task[T] extends Callable[T] {

  def dl: Int

  var span: Int = dl

  var cycle: Int = _

  var fixSeconds = 0

  def fixSeconds_(fs: Int): Task[T] = {
    fixSeconds = fs
    this
  }

  def seconds: Int = TIME_UNIT match {
    case TimeUnit.SECONDS =>
      span % 60
    case TimeUnit.MINUTES | TimeUnit.HOURS =>
      0
  }

  def minutes: Int = TIME_UNIT match {
    case TimeUnit.SECONDS =>
      (span / 60) % 60
    case TimeUnit.MINUTES =>
      span % 60
    case TimeUnit.HOURS =>
      0
  }

  def hours: Int = TIME_UNIT match {
    case TimeUnit.SECONDS =>
      span / 3600
    case TimeUnit.MINUTES =>
      span / 60
    case TimeUnit.HOURS =>
      span
  }


  def setSpan(sp: Int): Task[T] = {
    span = sp
    this
  }

  val createTime: (Int, Int, Int, Int) = {
    val calendar: Calendar = Calendar.getInstance
    calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    calendar.setTime(new Date())
    val startSecond = calendar.get(Calendar.SECOND)
    val startMinute = calendar.get(Calendar.MINUTE)
    val startHour = calendar.get(Calendar.HOUR)
    val startMill = calendar.get(Calendar.MILLISECOND)
    (startHour, startMinute, startSecond, startMill)
  }

  var createTrigger: (Int, Int) = (0, 0)

  def persist(): Boolean

  private val time: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new Date())
  }

  override def toString: String = s" task $time, cycle : $cycle,dl :$dl ,span : $span , seconds : $seconds ,minutes :$minutes , hours : $hours"


}
