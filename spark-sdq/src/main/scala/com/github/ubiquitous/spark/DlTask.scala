package com.github.ubiquitous.spark

import com.github.ubiquitous.spark.kafka.DelayProducerTaskImpl
import org.apache.kafka.clients.producer.Producer

/**
  *
  * @author Namhwik on 2020-05-13 18:21
  */
object DlTask {
  def apply(dl: Int, k: String, msg: String)(implicit producer: Producer[String, String]) =
    new DelayProducerTaskImpl(dl, k, msg)
}
