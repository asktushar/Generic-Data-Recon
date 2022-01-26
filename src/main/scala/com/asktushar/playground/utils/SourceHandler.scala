package com.asktushar.playground.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException

class SourceHandler {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def readJSONFile(spark: SparkSession, filePath : String) : DataFrame = {
    try {
      spark.read.json(filePath)
    } catch {
      case e: FileNotFoundException =>
        logger.warn(s"File $filePath Not found: ", e)
        throw new FileNotFoundException()
    }
  }
  def readMultilineJSONFile(spark: SparkSession, filePath : String) : DataFrame = {
    try {
      spark.read.option("multiline","true").json(filePath)
    } catch {
      case e: FileNotFoundException =>
        logger.warn(s"File $filePath Not found: ", e)
        throw new FileNotFoundException()
    }
  }
}
