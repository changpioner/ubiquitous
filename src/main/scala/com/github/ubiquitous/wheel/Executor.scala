package com.github.ubiquitous.wheel

import java.util.concurrent.{ExecutorService, Executors}

/**
  *
  * @author Namhwik on 2020-04-28 17:37
  */
object Executor {
  private val executorService: ExecutorService = Executors.newFixedThreadPool(10)

  def submit(task: Runnable): Unit = {
    executorService.submit(task)
  }

}
