package com.asktushar.playground.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkSessionAware {
  def appName: String

  def extraConfigs: Map[String, String] = Map()

  implicit lazy val spark: SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    extraConfigs
      .foldLeft(builder)((b, e) => b.config(e._1, e._2))
      .getOrCreate()
  }

  implicit lazy val sc: SparkContext = spark.sparkContext
}
