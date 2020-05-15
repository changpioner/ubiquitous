package com.github.ubiquitous.spark.entrance

import com.github.ubiquitous.spark.DlTask
import com.github.ubiquitous.spark.util.KafkaSink
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.spark.sql.SparkSession

/**
  *
  * @author Namhwik on 2020-05-13 18:18
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("test")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.range(100).map(_.toString)
    // val kafkaSink = spark.sparkContext.broadcast(new KafkaSink(() => new KafkaProducer[String, String](new java.util.Properties())))
    ds.foreach(s => DlTask(s.toInt, s, s)(null.asInstanceOf[KafkaProducer[String, String]]).schedule())
    ds.show()
  }
}
