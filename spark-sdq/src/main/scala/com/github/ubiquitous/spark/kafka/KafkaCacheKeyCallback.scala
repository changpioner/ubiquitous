package com.github.ubiquitous.spark.kafka

import java.util.concurrent.Callable

import com.github.ubiquitous.exe.Executor
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-13 15:00
  */
class KafkaCacheKeyCallback[KT, F](scheduleCacheKey: KT, f: KT => F) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      Logger.getLogger(this.getClass).error(s"recleaned msg send error scheduleCacheKey : $scheduleCacheKey," + exception.getMessage)
    } else {
      Executor.submit(f)
    }
  }
}