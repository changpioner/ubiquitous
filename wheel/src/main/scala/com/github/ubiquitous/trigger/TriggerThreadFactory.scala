package com.github.ubiquitous.trigger

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-09 16:57
  */
class TriggerThreadFactory extends ThreadFactory {
  val logger: Logger = Logger.getLogger(this.getClass)
  var count = new AtomicInteger(-1)

  override def newThread(r: Runnable): Thread = {
    val thread = new Thread(r, s"Trigger-${count.incrementAndGet()}")
    thread.setDaemon(true)
    logger.info(s"Started trigger thread : ${thread.getName}")
    thread
  }
}
