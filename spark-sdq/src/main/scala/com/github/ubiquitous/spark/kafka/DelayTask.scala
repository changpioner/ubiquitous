package com.github.ubiquitous.spark.kafka

import com.github.ubiquitous.config.Conf.config
import com.github.ubiquitous.utils.CacheUtil
import com.github.ubiquitous.wheel.{Task, WheelFactory}
import org.apache.log4j.Logger

/**
  *
  * @author Namhwik on 2020-05-13 14:43
  */
trait DelayTask[K, V] extends Task[Unit] with CacheUtil {

  def msg_(m: V): Unit

  def msg: V

  def k: K

  val logger: Logger

  override def persist(): Boolean = {

    val KEY_CACHE_TABLE: String = config.getString("cache.table")
    val KEY_CACHE_FAMILY: String = config.getString("cache.family")
    val KEY_CACHE_DELAY: String = config.getString("cache.delay")
    val PERSIST_V: Boolean = config.getBoolean("cache.ifPersistValue")

    val CH_COL_META: String = config.getString("cache.meta")
    val CH_COL_TIME: String = config.getString("cache.time")

    !PERSIST_V || {
      try {
        insert(KEY_CACHE_TABLE, k, KEY_CACHE_FAMILY,
          Map(
            CH_COL_TIME -> System.currentTimeMillis().toString,
            KEY_CACHE_DELAY -> dl.toString,
            CH_COL_META -> msg
          )
        )
        clearV(k, msg)
        true
      } catch {
        case ex: Exception =>
          // log the exception msg
          logger.error(s" error occurred when persist delay task , msg : ${ex.getMessage}")
          false
      }
    }
  }

  def clearV[KT, VT](key: KT, v: VT): Unit = msg_(null.asInstanceOf[V])

  def schedule(): Unit = WheelFactory.addDelayTask(this)

}
