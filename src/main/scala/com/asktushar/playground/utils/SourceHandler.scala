package com.asktushar.playground.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class SourceHandler {
  def readJSONFile(spark: SparkSession, filePath : String) : DataFrame = {
    spark.read.json(filePath)
  }
  def readMultilineJSONFile(spark: SparkSession, filePath : String) : DataFrame = {
    spark.read.option("multiline","true").json(filePath)
  }
}
