package com.asktushar.playground.jobs

import com.asktushar.playground.utils.{SourceHandler, TargetHandler}
import com.asktushar.playground.SparkContextSpec
import com.asktushar.playground.conf.ArgConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

class ComplaintReportTest extends WordSpec with Matchers with SparkContextSpec {
  private val argConf: ArgConf = ArgConf("ComplaintReport", "local[1]", "complaints.jsonl", "category_names.json", "service_names.json","output")
  private val targetHandler = new TargetHandler
  private val transformationHandler = new TransformationHandler
  private val sourceHandler = new SourceHandler
  private val complaintReport = new ComplaintReport(argConf,targetHandler,transformationHandler,sourceHandler)

  "Testing the business logic to join 3 files" in withSparkSession { sparkSession =>
    //Category DF
    val categoryRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("249", "\"Other (none of the above)\""),
      Row("63", "\"Billing\"")
    ))
    val categorySchemaExpected = StructType(
      Array(
        StructField("category_id", StringType, true),
        StructField("category_name", StringType, true)
      )
    )
    val categoryDF = sparkSession.createDataFrame(categoryRowsExpected, categorySchemaExpected)

    //Service DF
    val serviceRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("53014", "\"Mobile\""),
      Row("409", "\"Mobile\"")
    ))
    val serviceSchemaExpected = StructType(
      Array(
        StructField("service_id", StringType, true),
        StructField("service_name", StringType, true)
      )
    )
    val serviceDF = sparkSession.createDataFrame(serviceRowsExpected, serviceSchemaExpected)

    //Complaint DF
    val complaintRows = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "53014", "249"),
      Row("\"19 August unfortunately balances cuts to make '1p' balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "409", "63")
    ))
    val complaintSchema = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val complaintDF = sparkSession.createDataFrame(complaintRows, complaintSchema)

    //Actual DF
    val actualDF = complaintReport.createComplaintReportDF(complaintDF, categoryDF, serviceDF)

    assert(actualDF.count() == 2)
  }


  "Testing the business logic to join 3 files with missing categories" in withSparkSession { sparkSession =>
    //Category DF
    val categoryRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("249", "\"Other (none of the above)\"")
    ))
    val categorySchemaExpected = StructType(
      Array(
        StructField("category_id", StringType, true),
        StructField("category_name", StringType, true)
      )
    )
    val categoryDF = sparkSession.createDataFrame(categoryRowsExpected, categorySchemaExpected)

    //Service DF
    val serviceRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("53014", "\"Mobile\""),
      Row("409", "\"Mobile\"")
    ))
    val serviceSchemaExpected = StructType(
      Array(
        StructField("service_id", StringType, true),
        StructField("service_name", StringType, true)
      )
    )
    val serviceDF = sparkSession.createDataFrame(serviceRowsExpected, serviceSchemaExpected)

    //Complaint DF
    val complaintRows = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "53014", "249"),
      Row("\"19 August unfortunately balances cuts to make '1p' balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "409", "63")
    ))
    val complaintSchema = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val complaintDF = sparkSession.createDataFrame(complaintRows, complaintSchema)

    //Actual DF
    val actualDF = complaintReport.createComplaintReportDF(complaintDF, categoryDF, serviceDF)

    assert(actualDF.count() == 2)
  }

  "Testing the business logic to join 3 files with missing service" in withSparkSession { sparkSession =>
    //Category DF
    val categoryRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("249", "\"Other (none of the above)\""),
      Row("63", "\"Billing\"")
    ))
    val categorySchemaExpected = StructType(
      Array(
        StructField("category_id", StringType, true),
        StructField("category_name", StringType, true)
      )
    )
    val categoryDF = sparkSession.createDataFrame(categoryRowsExpected, categorySchemaExpected)

    //Service DF
    val serviceRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("53014", "\"Mobile\"")
    ))
    val serviceSchemaExpected = StructType(
      Array(
        StructField("service_id", StringType, true),
        StructField("service_name", StringType, true)
      )
    )
    val serviceDF = sparkSession.createDataFrame(serviceRowsExpected, serviceSchemaExpected)

    //Complaint DF
    val complaintRows = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "53014", "249"),
      Row("\"19 August unfortunately balances cuts to make '1p' balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "409", "63")
    ))
    val complaintSchema = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val complaintDF = sparkSession.createDataFrame(complaintRows, complaintSchema)

    //Actual DF
    val actualDF = complaintReport.createComplaintReportDF(complaintDF, categoryDF, serviceDF)

    assert(actualDF.count() == 2)
  }

  "Testing the business logic to join 3 files with missing service and category" in withSparkSession { sparkSession =>
    //Category DF
    val categoryRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("249", "\"Other (none of the above)\"")
    ))
    val categorySchemaExpected = StructType(
      Array(
        StructField("category_id", StringType, true),
        StructField("category_name", StringType, true)
      )
    )
    val categoryDF = sparkSession.createDataFrame(categoryRowsExpected, categorySchemaExpected)

    //Service DF
    val serviceRowsExpected = sparkSession.sparkContext.parallelize(Seq(
      Row("53014", "\"Mobile\"")
    ))
    val serviceSchemaExpected = StructType(
      Array(
        StructField("service_id", StringType, true),
        StructField("service_name", StringType, true)
      )
    )
    val serviceDF = sparkSession.createDataFrame(serviceRowsExpected, serviceSchemaExpected)

    //Complaint DF
    val complaintRows = sparkSession.sparkContext.parallelize(Seq(
      Row("\"I had a mobile account from July 2012 to Jul 2014. My account was in credit when I cancelled my contract, but I have never received a refund.\"", "\"2018-08-29 14:03:41\"", "53014", "249"),
      Row("\"19 August unfortunately balances cuts to make '1p' balance maybe overcharged can you check and refund my balance please \"", "\"2018-08-23 17:30:34\"", "409", "63")
    ))
    val complaintSchema = StructType(
      Array(
        StructField("text", StringType, true),
        StructField("created_at", StringType, true),
        StructField("service_id", StringType, true),
        StructField("category_id", StringType, true)
      )
    )
    val complaintDF = sparkSession.createDataFrame(complaintRows, complaintSchema)

    //Actual DF
    val actualDF = complaintReport.createComplaintReportDF(complaintDF, categoryDF, serviceDF)

    assert(actualDF.count() == 2)
  }
}
