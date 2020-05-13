package com.github.ubiquitous.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

/**
  *
  * @author Namhwik on 2020-05-11 15:24
  */
object Conf {

  val config: Config = ConfigFactory.load("properties")


  final val TIME_UNIT = config.getString("unit.granularity") match {
    case "seconds" => TimeUnit.SECONDS
    case "minutes" => TimeUnit.MINUTES
    case "hours" => TimeUnit.HOURS
    case _ => null
  }

  assertSettings()


  def getString(k: String): String =
    config.getString(s"unit.$k")

  def getInt(k: String): Int =
    config.getInt(s"unit.$k")


  def assertSettings(): Unit = {
    require(TIME_UNIT != null,
      s"Time granularity does not support : ${config.getString("unit.granularity")}")
  }


}
