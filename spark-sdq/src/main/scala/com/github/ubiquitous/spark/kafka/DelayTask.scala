package com.github.ubiquitous.spark.kafka

import com.github.ubiquitous.config.Conf.config
import com.github.ubiquitous.utils.HbaseUtil
import com.github.ubiquitous.wheel.{Task, WheelFactory}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-13 14:43
  */
abstract class DelayTask[K, V](dl: Int, k: K, var msg: V)
  extends Task[Unit](dl: Int) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private final val KEY_CACHE_TABLE = config.getString("cache.table")
  private final val KEY_CACHE_FAMILY = config.getString("cache.family")
  private final val KEY_CACHE_DELAY = config.getString("cache.delay")
  private val PERSIST_V: Boolean = config.getBoolean("cache.ifPersistValue")

  private final val CH_COL_META = "meta"
  private final val CH_COL_TIME = "time"


  override def persist(): Boolean = {
    !PERSIST_V || {
      try {
        HbaseUtil.insertV2(KEY_CACHE_TABLE, k, KEY_CACHE_FAMILY,
          Map(
            CH_COL_TIME -> System.currentTimeMillis(),
            KEY_CACHE_DELAY -> dl,
            CH_COL_META -> msg
          )
        )
        true
      } catch {
        case ex: Exception =>
          // log the exception msg
          logger.error(s" error occurred when persist delay task , msg : ${ex.getMessage}")
          false
      }
    }
  }

  def persistKey[KT, VT](key: KT, v: VT): Unit = {

    msg = null.asInstanceOf[V]
  }

  def removeKey[KT](key: KT): Unit = {
    HbaseUtil.deleteV2(KEY_CACHE_TABLE, key)
  }

  def schedule(): Unit = {
    WheelFactory.addDelayTask(this)
  }

}
