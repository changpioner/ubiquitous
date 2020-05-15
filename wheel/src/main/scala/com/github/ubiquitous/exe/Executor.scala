package com.github.ubiquitous.exe

import java.util.concurrent._

import com.github.ubiquitous.config.Conf
import com.github.ubiquitous.trigger.{Trigger, TriggerThreadFactory}

import monix.eval.Task
import monix.execution.schedulers.ExecutionModel
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import org.apache.log4j.Logger

import scala.concurrent._
import scala.concurrent.{Future => SFuture}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal

/**
  *
  * @author Namhwik on 2020-04-28 17:37
  */
object Executor {

  val MAX_POOL_SIZE: Int = Conf.getInt("maximumPoolSize")

  require(MAX_POOL_SIZE > 0)

  private final val TRIGGER_CORE_SIZE = Conf.getString("triggerMod") match {
    case "single" => 1
    case "multiple" => 3
    case _ => throw new IllegalArgumentException(s"trigger mod does not support ${Conf.getString("triggerMod")}")
  }


  private implicit val coreScheduler: Scheduler = Scheduler.apply(
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(2, MAX_POOL_SIZE, 10L,
        TimeUnit.SECONDS, new SynchronousQueue[Runnable]())
    ))
  //根据延时任务的精度控制池子的大小
  private val triggerScheduler = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(TRIGGER_CORE_SIZE, TRIGGER_CORE_SIZE, 2L,
      TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), new TriggerThreadFactory)
  )

  def submit(task: Runnable): Unit = executeBlockingIO(task.run())

  def submit[T](task: Callable[T]): SFuture[T] = executeBlockingIO(task.call())

  def submit[T](f: => T): SFuture[T] = executeBlockingIO(f)

  def addTrigger(trigger: Runnable): Unit = triggerScheduler.execute(trigger)


  def executeBlockingIO[T](cb: => T): SFuture[T] = {
    val p = Promise[T]()
    coreScheduler.execute(new Runnable {
      def run(): Unit = try {
        p.success(blocking(cb))
      }
      catch {
        case NonFatal(ex) =>
          Logger.getLogger(this.getClass).error(s"Uncaught I/O exception", ex)
          p.failure(ex)
      }
    })
    p.future
  }
}
