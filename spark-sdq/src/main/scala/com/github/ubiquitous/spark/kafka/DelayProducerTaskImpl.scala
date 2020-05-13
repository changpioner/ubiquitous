package com.github.ubiquitous.spark.kafka

import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-13 18:00
  */


class DelayProducerTaskImpl(dl: Int, k: String, msg: String)(implicit producer: Producer[String, String]) extends
  DelayTask[String, String](dl: Int, k: String, msg: String) {
  val logger: Logger = Logger.getLogger(this.getClass)

  override def call(): Unit = {
    //    producer.send(
    //      new ProducerRecord[String, String](k, msg),
    //      new KafkaCacheKeyCallback[String, Unit](k, removeKey)
    //    )
    logger.info(s"sended : [$k , $msg]")
  }
}
