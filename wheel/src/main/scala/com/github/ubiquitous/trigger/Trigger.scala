package com.github.ubiquitous.trigger

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


  private val logger = Logger.getLogger(this.getClass)

  private var start: Boolean = _

  implicit val executor: Executor.type = Executor
  implicit val wheelFactory: WheelFactory.type = WheelFactory


  private var s: Int = 0
  private var m: Int = 0
  //private var h: Int = 0

  override def run(): Unit = {
    start = true
    while (start) {
      step().foreach(inWheel)
      timeUnit.sleep(1)
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
          case TimeUnit.MINUTES => TimeUnit.SECONDS
          case TimeUnit.HOURS => TimeUnit.MINUTES
          case _ => throw new IllegalArgumentException(s"unsupported $timeUnit ")
        }
      ).get.addTask(task))
    else
      runTasks(tasks)
  }

  def runTasks(tasks: Set[Task[Any]])(implicit executor: Executor.type): Unit = {
    tasks.foreach(t => executor.submit(t))
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
      val index = if (tick.compareAndSet(wheel.bufferSize - 1, 0))
        0
      else
      //Total tick number of records
        tick.incrementAndGet()
      val task = wheel.remove(index)
      logger.debug(s"current running index :$index")
      if (task != null && task.nonEmpty)
        logger.debug(s"Got tasks : ${if (task == null) "null" else task.mkString(" , ")}")
      if (task != null && task.nonEmpty) {
        tm match {
          case TimeUnit.SECONDS =>
            logger.debug("submit task ...")
            task.foreach(t => Executor.submit(t))
          case TimeUnit.MINUTES =>
            logger.debug("moving task to seconds wheel ...")
            val (nextWheelTasks: Set[Task[Any]], toRunTasks) = task.map(t => t.setSpan(t.seconds)).partition(_.span > 0)
            runTasks(toRunTasks)
            push(tm, nextWheelTasks)
          case TimeUnit.HOURS =>
            logger.debug("moving task to minutes wheel ...")
            val (nextWheelTasks, toSecondTasks) = task.map(t => t.setSpan(t.minutes)).partition(_.span > 0)
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
      if (s + 1 == 60) s = 0 else s += 1
      s
    }

    def addM(): Int = {
      if (m + 1 == 60) m = 0 else m += 1
      m
    }


    TIME_UNIT match {
      case TimeUnit.SECONDS =>
        addS() match {
          case 0 =>
            List(wheelFactory.unitWheel(TIME_UNIT).get, wheelFactory.unitWheel(TimeUnit.MINUTES).get) ::: {
              addM() match {
                case 0 => List(wheelFactory.unitWheel(TimeUnit.HOURS).get)
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
