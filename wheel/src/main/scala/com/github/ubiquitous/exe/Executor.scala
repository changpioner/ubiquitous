package com.github.ubiquitous.exe

import java.util.concurrent._
import com.github.ubiquitous.config.Conf
import com.github.ubiquitous.trigger.{Trigger, TriggerThreadFactory}

/**
  *
  * @author Namhwik on 2020-04-28 17:37
  */
object Executor {

  private final val TRIGGER_CORE_SIZE = Conf.getString("triggerMod") match {
    case "single" => 1
    case "multiple" => 3
    case _ => throw new IllegalArgumentException(s"trigger mod does not support ${Conf.getString("triggerMod")}")
  }


  private val executorService: ExecutorService =
    new ThreadPoolExecutor(4, 15, 10L,
      TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](2))

  //根据延时任务的精度控制池子的大小
  private val triggerExecutor = new ThreadPoolExecutor(TRIGGER_CORE_SIZE, TRIGGER_CORE_SIZE, 0L,
    TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), new TriggerThreadFactory)

  def submit(task: Runnable): Unit = {
    executorService.submit(task)
  }

  def submit[T](task: Callable[T]): Future[T] = {
    executorService.submit(task)
  }

  def addTrigger(trigger: Runnable): Unit = {
    triggerExecutor.submit(trigger)
  }


}
