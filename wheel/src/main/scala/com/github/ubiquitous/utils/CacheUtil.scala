package com.github.ubiquitous.utils

/**
  *
  * @author Namhwik on 2020-06-17 15:59
  */
trait CacheUtil {

  def insert(tableName: String, rowKey: Any, family: String, values: Map[String, Any])

  def delete[KT](key: KT)

}
