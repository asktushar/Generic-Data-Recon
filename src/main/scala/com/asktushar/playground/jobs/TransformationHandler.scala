package com.asktushar.playground.jobs

import org.apache.spark.sql.{Column, DataFrame, functions}

class TransformationHandler {
  def unpivotDF(df : DataFrame, schema : List[String]) : DataFrame = {
    val stackExprServiceMap = s"stack(${df.columns.length}," + df.columns
      .flatMap(c => Seq(c, s"`$c`"))
      .mkString(",") + s") as (${schema.mkString(",")})"

    df.selectExpr(stackExprServiceMap)
  }

  def cleanDF(df : DataFrame, column: Column) : DataFrame = {
    df.withColumn("temp_text_cleaned", functions.regexp_replace(column, """(?<!^)\\"(?!$)""", "'"))
      .drop(column)
      .withColumnRenamed("temp_text_cleaned",column.toString())
  }
}
