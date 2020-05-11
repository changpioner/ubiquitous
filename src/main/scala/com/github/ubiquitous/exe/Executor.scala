package com.github.ubiquitous.exe

import java.util.concurrent._

import com.github.ubiquitous.trigger.{Trigger, TriggerThreadFactory}

/**
  *
  * @author Namhwik on 2020-04-28 17:37
  */
object Executor {

  private val executorService: ExecutorService =
    new ThreadPoolExecutor(4, 15, 10L,
      TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]())

  //TODO 根据延时任务的精度控制池子的大小
  private val triggerExecutor = new ThreadPoolExecutor(3, 3, 0L,
    TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), new TriggerThreadFactory)

  def submit(task: Runnable): Unit = {
    executorService.submit(task)
  }

  def submit[T](task: Callable[T]): Future[T] = {
    executorService.submit(task)
  }

  def addTrigger(trigger: Trigger): Unit = {
    triggerExecutor.submit(trigger)
  }


}
