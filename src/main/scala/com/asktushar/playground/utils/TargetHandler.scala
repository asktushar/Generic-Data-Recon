package com.asktushar.playground.utils

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException

class TargetHandler {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def writeFinalDF(complaintsReport: DataFrame, filePath: String) : Unit = {
    try {
      complaintsReport.write.option("header",value = true)
        .mode(SaveMode.Append)
        .option("delimiter", ",")
        .csv(filePath)
    } catch {
      case e : FileNotFoundException =>
        logger.warn(s"File $filePath Not found: ", e)
        throw new FileNotFoundException(s"File $filePath Not found: ")
    }
  }
}
