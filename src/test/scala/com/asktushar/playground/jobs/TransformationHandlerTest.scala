package com.asktushar.playground.jobs


import com.asktushar.playground.SparkContextSpec
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

class TransformationHandlerTest extends WordSpec with Matchers with SparkContextSpec {
  private val transformationHandler = new TransformationHandler
  private val serviceMapSchema = List("service_id", "service_name")
  private val categoryMapSchema = List("category_id", "category_name")

  "Testing unpivotDF to return unPivoted categoryMap DF" in withSparkSession { sparkSession =>
    //Input DF
    val rowsInput = sparkSession.sparkContext.parallelize(Seq(Row("\"Other (none of the above)\"", "\"Billing\"")))
    val schemaInput = StructType(
      Array(
        StructField("249", StringType, true),
        StructField("63", StringType, true)
      )
    )
    val inputDF = sparkSession.createDataFrame(rowsInput, schemaInput)

    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("249", "\"Other (none of the above)\""),
      Row("63", "\"Billing\"")
    ))
    val schemaExpected = StructType(
      Array(
        StructField("category_id", StringType, true),
        StructField("category_name", StringType, true)
      )
    )
    val expectedDF = sparkSession.createDataFrame(rowsExpected, schemaExpected)

    //Actual DF
    val actualDF = transformationHandler.unpivotDF(inputDF,categoryMapSchema)

    assert(actualDF.except(expectedDF).count() == 0)
  }


  "Testing unpivotDF to return unPivoted serviceMap DF" in withSparkSession { sparkSession =>
    //Input DF
    val rowsInput = sparkSession.sparkContext.parallelize(Seq(Row("\"Mobile\"", "\"Mobile\"")))
    val schemaInput = StructType(
      Array(
        StructField("53014", StringType, true),
        StructField("409", StringType, true)
      )
    )
    val inputDF = sparkSession.createDataFrame(rowsInput, schemaInput)

    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("53014", "\"Mobile\""),
      Row("409", "\"Mobile\"")
    ))
    val schemaExpected = StructType(
      Array(
        StructField("service_id", StringType, true),
        StructField("service_name", StringType, true)
      )
    )
    val expectedDF = sparkSession.createDataFrame(rowsExpected, schemaExpected)

    //Actual DF
    val actualDF = transformationHandler.unpivotDF(inputDF,serviceMapSchema)

    assert(actualDF.except(expectedDF).count() == 0)
  }

  "Testing cleanDF to return cleaned complaints DF" in withSparkSession { sparkSession =>
    //Input DF
    val rowsInput = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "409", "63"),
      Row("\"19 August unfortunately balances cuts to make \\\"1p\\\" balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "349", "249")
    ))
    val schemaInput = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val inputDF = sparkSession.createDataFrame(rowsInput, schemaInput)

    //Expected DF
    val rowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "409", "63"),
      Row("\"19 August unfortunately balances cuts to make '1p' balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "349", "249")
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
    val actualDF = transformationHandler.cleanDF(inputDF,inputDF.col("text"))

    assert(actualDF.select("text").except(expectedDF.select("text")).count() == 0)
    assert(actualDF.select("text").except(inputDF.select("text")).count() == 1)
  }

  "Testing cleanDF to return same complaints DF if no cleaning is required" in withSparkSession { sparkSession =>
    //Input DF
    val rowsInput = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "409", "63"),
      Row("\"19 August unfortunately balances cuts to make 1p balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "349", "249")
    ))
    val schemaInput = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val inputDF = sparkSession.createDataFrame(rowsInput, schemaInput)

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
    val actualDF = transformationHandler.cleanDF(inputDF,inputDF.col("text"))

    assert(actualDF.select("text").except(expectedDF.select("text")).count() == 0)
    assert(actualDF.select("text").except(inputDF.select("text")).count() == 0)
  }

}




