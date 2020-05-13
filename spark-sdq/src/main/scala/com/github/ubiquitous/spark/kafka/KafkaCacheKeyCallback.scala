package com.github.ubiquitous.spark.kafka

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-13 15:00
  */
class KafkaCacheKeyClean(scheduleCacheKey: String, hbaseConn: HbaseUtil2) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      Logger.getLogger("org.apache.spark").error(s"recleaned msg send error scheduleCacheKey : $scheduleCacheKey," + exception.getMessage)
    } else {
      Task {
        try {
          hbaseConn.delete(SCHEDULE_CACHE, scheduleCacheKey)
        } catch {
          case ex: Exception =>
            Logger.getLogger(this.getClass).warn(s"Delete $SCHEDULE_CACHE:$scheduleCacheKey failed , ${ex.getMessage}")
        }
      }.runAsync
    }
  }
}