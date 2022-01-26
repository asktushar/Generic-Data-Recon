package com.asktushar.playground.utils

import com.asktushar.playground.SparkContextSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

class SourceHandlerTest extends WordSpec with Matchers with SparkContextSpec {

  private val sourceHandler = new SourceHandler
  private val complaintsSourcePath = "src/test/resources/input/complaints.jsonl"
  private val categorySourcePath = "src/test/resources/input/category_names.json"
  private val serviceSourcePath = "src/test/resources/input/service_names.json"

  "Testing SourceHandlerTest for reading correct complaintsSource" in withSparkSession { sparkSession =>
    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "409", "63"),
      Row("\"19 August unfortunately balances cuts to make 1p balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "349", "249")
    ))
    val schemaExpected = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val expectedDF = sparkSession.createDataFrame(rowsExpected, schemaExpected)

    //Actual DF
    val actualDF = sourceHandler.readJSONFile(sparkSession,complaintsSourcePath)

    assert(actualDF.count() == 2)
    assert(actualDF.select("text","created_at", "service_id", "category_id").except(expectedDF.select("text","created_at", "service_id", "category_id")).count() == 0)
  }

  "Testing SourceHandlerTest for reading correct categorySource" in withSparkSession { sparkSession =>
    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(Row("\"Other (none of the above)\"", "\"Billing\"")))
    val schemaExpected = StructType(
      Array(
        StructField("249", StringType, true),
        StructField("63", StringType, true)
      )
    )
    val expectedDF = sparkSession.createDataFrame(rowsExpected, schemaExpected)

    //Actual DF
    val actualDF = sourceHandler.readMultilineJSONFile(sparkSession,categorySourcePath)

    assert(actualDF.count() == 1)
    assert(actualDF.select("249","63").except(expectedDF.select("249","63")).count() == 0)
  }

  "Testing SourceHandlerTest for reading correct serviceSource" in withSparkSession { sparkSession =>
    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(Row("\"Mobile\"", "\"Mobile\"")))
    val schemaExpected = StructType(
      Array(
        StructField("53014", StringType, true),
        StructField("409", StringType, true)
      )
    )
    val expectedDF = sparkSession.createDataFrame(rowsExpected, schemaExpected)

    //Actual DF
    val actualDF = sourceHandler.readMultilineJSONFile(sparkSession,serviceSourcePath)

    assert(actualDF.count() == 1)
    assert(actualDF.select("53014","409").except(expectedDF.select("53014","409")).count() == 0)
  }

}
