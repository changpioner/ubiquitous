package com.github.ubiquitous.trigger

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone, Calendar}
import java.util.concurrent.TimeUnit

import com.github.ubiquitous.exe.Executor
import com.github.ubiquitous.wheel.{Task, Wheel, WheelFactory}
import org.apache.log4j.Logger
import com.github.ubiquitous.config.Conf.TIME_UNIT


/**
  *
  * @author Namhwik on 2020-04-15 15:49
  */
class Trigger(timeUnit: TimeUnit) extends Runnable {


  private val calendar: Calendar = Calendar.getInstance
  calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"))
  calendar.setTime(new Date())
  private val logger = Logger.getLogger(this.getClass)

  private var start: Boolean = _
  private var time: String = _

  implicit val executor: Executor.type = Executor
  implicit val wheelFactory: WheelFactory.type = WheelFactory

  var triggerSec: Int = calendar.get(Calendar.SECOND)
  var triggerMin: Int = calendar.get(Calendar.MINUTE)
  var triggerHour: Int = calendar.get(Calendar.HOUR_OF_DAY)

  override def run(): Unit = {
    start = true
    time = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
      sdf.format(new Date())
    }
    logger.info(s"[ $time ] Task trigger : $timeUnit is started !")
    while (start) {
      timeUnit.sleep(1)
      step().foreach(inWheel)
      calendar.setTime(new Date())
      triggerSec = calendar.get(Calendar.SECOND)
      triggerMin = calendar.get(Calendar.MINUTE)
    }
    logger.info(s"Task trigger : $timeUnit is stopped !")

  }

  /**
    * Push the tasks to next level wheel
    * If the accuracy of the time wheel is less than the set accuracy,
    * run the tasks directly, else move the task to the next level time wheel
    *
    * @param timeUnit     time unit of current wheel
    * @param tasks        tasks to be moved
    * @param wheelFactory WheelFactory
    */
  def push(timeUnit: TimeUnit, tasks: Set[Task[Any]])(implicit wheelFactory: WheelFactory.type): Unit = {
    logger.debug("moving task to next wheel ...")
    if ( {
      // time unit of next level wheel
      timeUnit match {
        case TimeUnit.MINUTES => TimeUnit.SECONDS
        case TimeUnit.HOURS => TimeUnit.MINUTES
        case _ => throw new IllegalArgumentException(s"unsupported $timeUnit ")
      }
    }.compareTo(TIME_UNIT) >= 0)
      tasks.foreach(task => wheelFactory.unitWheel(
        timeUnit match {
          case TimeUnit.MINUTES =>
            if (task.minutes > 0) TimeUnit.MINUTES else TimeUnit.SECONDS
          case TimeUnit.HOURS =>
            if (task.hours > 0) TimeUnit.HOURS else TimeUnit.MINUTES
          case _ => throw new IllegalArgumentException(s"unsupported $timeUnit ")
        }
      ).get.addTask(task))
    else
      runTasks(tasks)
  }

  def runTasks(tasks: Set[Task[Any]])(implicit executor: Executor.type): Unit = {
    tasks.foreach(t => executor.submit(t))
  }


  def fixSpan[T](t: Task[T], timeUnit: TimeUnit): Task[T] = {
    logger.debug(s"start fix $t , created at ${t.createTime}")

    timeUnit match {
      case TimeUnit.MINUTES =>
        //更改从分钟时间轮到秒时间轮的步数
        calendar.setTime(new Date())
        val nowMinute = calendar.get(Calendar.MINUTE)
        val nowSec = calendar.get(Calendar.SECOND)
        val readyMinute = t.createTime._2 + t.minutes
        val readySec = t.createTime._3 + t.seconds
        val spendMinute = if (readyMinute - nowMinute > 0) readyMinute - nowMinute else 0
        val spendSec = if (readySec - nowSec >= 0) readySec - nowSec else 60 - readySec + nowSec
        val seconds = spendMinute * 60 + spendSec
        TIME_UNIT match {
          case TimeUnit.SECONDS => t.setSpan(seconds)
          case TimeUnit.MINUTES => t.setSpan(0)
          case TimeUnit.HOURS => t.setSpan(0)
        }
      case TimeUnit.HOURS =>
        //更改从小时时间轮到分钟时间轮的步数
        val minutes = 0
        //if (t.createTime._2 >= startMinute)
        //t.minutes + t.createTime._2 - startMinute
        //else
        // t.minutes + 60 + t.createTime._2 - startMinute
        TIME_UNIT match {
          case TimeUnit.SECONDS => t.setSpan(minutes * 60 + t.seconds)
          case TimeUnit.MINUTES => t.setSpan(minutes)
          case TimeUnit.HOURS => t.setSpan(0)
        }
    }
  }

  /**
    * Get tasks of the wheel , and partition the task list to run tasks and push tasks,
    * run tasks will submit to the executor , push tasks will be pushed to next level wheel
    *
    * @param wheel Wheel
    */
  def inWheel(wheel: Wheel): Unit = {


    //time unit of the wheel
    val tm = wheel.getTimeUnit
    try {
      val tick = wheel.tick
      logger.debug(s"start add index task at ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())}")
      val index = if (tick.compareAndSet(wheel.bufferSize - 1, 0))
        0
      else
      //Total tick number of records
        tick.incrementAndGet()
      logger.debug(s"start remove $index task at ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())}")
      val task = wheel.remove(index)
      logger.debug(s"current running index :$index")
      if (task != null && task.nonEmpty)
        logger.debug(s"Got tasks at ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())}: ${if (task == null) "null" else task.mkString(" , ")}")
      if (task != null && task.nonEmpty) {
        tm match {
          case TimeUnit.SECONDS =>
            logger.debug("submit task ...")
            task.foreach(t => Executor.submit(t))
          case TimeUnit.MINUTES =>
            logger.debug("moving task to seconds wheel ...")
            val (nextWheelTasks: Set[Task[Any]], toRunTasks) = task.map(fixSpan(_, tm)).partition(_.span > 0)
            runTasks(toRunTasks)
            push(tm, nextWheelTasks)
          case TimeUnit.HOURS =>
            logger.debug("moving task to minutes wheel ...")
            val (nextWheelTasks, toSecondTasks) = task.map(fixSpan(_, tm)).partition(_.span > 0)
            push(tm, nextWheelTasks)
            val (secondTasks, toRunTasks) = toSecondTasks.map(t => t.setSpan(t.seconds)).partition(_.span > 0)
            push(TimeUnit.MINUTES, secondTasks)
            runTasks(toRunTasks)
          case _ =>
        }
      }


    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {

    }

  }


  /**
    * Start stepping from the time wheel at the lowest latitude
    *
    * @return List[Wheel] List of rotating time wheels
    */
  def step(): List[Wheel] = {

    def addS(): Int = {
      if (triggerSec + 1 == 60) triggerSec = 0 else triggerSec += 1
      triggerSec
    }

    def addM(): Int = {
      if (triggerMin + 1 == 60) triggerMin = 0 else triggerMin += 1
      triggerMin
    }

    def addH(): Int = {
      if (triggerHour == 23) triggerHour = 0 else triggerHour += 1
      triggerHour
    }


    TIME_UNIT match {
      case TimeUnit.SECONDS =>
        addS() match {
          case 0 =>
            List(wheelFactory.unitWheel(TIME_UNIT).get, wheelFactory.unitWheel(TimeUnit.MINUTES).get) ::: {
              addM() match {
                case 0 =>
                  addH()
                  List(wheelFactory.unitWheel(TimeUnit.HOURS).get)
                case _ => Nil
              }
            }
          case _ =>
            List(wheelFactory.unitWheel(TIME_UNIT).get)
        }

      case TimeUnit.MINUTES =>
        addM() match {
          case 0 => List(wheelFactory.unitWheel(TIME_UNIT).get, wheelFactory.unitWheel(TimeUnit.HOURS).get)
          case _ => List(wheelFactory.unitWheel(TIME_UNIT).get)
        }
      case TimeUnit.HOURS =>
        List(wheelFactory.unitWheel(TimeUnit.HOURS).get)
      case _ => Nil
    }


  }


  def stop(): Unit = start = false


}
