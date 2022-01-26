package com.asktushar.playground.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

class TargetHandler {
  def writeFinalDF(complaintsReport: DataFrame, filePath: String) : Unit = {
    complaintsReport.write.option("header",value = true)
      .mode(SaveMode.Append)
      .option("delimiter", ",")
      .csv(filePath)
  }
}
