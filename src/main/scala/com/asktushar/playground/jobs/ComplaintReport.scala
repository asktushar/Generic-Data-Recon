package com.asktushar.playground.jobs

import com.asktushar.playground.utils.{SourceHandler, SparkSessionAware, TargetHandler}
import com.asktushar.playground.conf.ArgConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

class ComplaintReport(argConf: ArgConf,
                      targetHandler: TargetHandler,
                      transformationHandler: TransformationHandler,
                      sourceHandler: SourceHandler) extends SparkSessionAware {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def appName: String = "ComplaintReport Batch Load"

  override def extraConfigs: Map[String, String] = Map("spark.master" -> argConf.master)

  private val serviceMapSchema = List("service_id", "service_name")
  private val categoryMapSchema = List("category_id", "category_name")

  def generateReport(): Unit = try {
    // Extracting Data
    val complaintsSource = sourceHandler.readJSONFile(spark, argConf.complaintsSourcePath)
    val categorySource = sourceHandler.readMultilineJSONFile(spark, argConf.categorySourcePath)
    val serviceSource = sourceHandler.readMultilineJSONFile(spark, argConf.serviceSourcePath)

    // Cleaning Data
    val complaintsSourceCleaned = transformationHandler.cleanDF(complaintsSource, complaintsSource.col("text"))
    val serviceMap = transformationHandler.unpivotDF(serviceSource, serviceMapSchema)
    val categoryMap = transformationHandler.unpivotDF(categorySource, categoryMapSchema)

    // Transforming Data
    val complaintsReport = createComplaintReportDF(complaintsSourceCleaned, categoryMap, serviceMap)

    //Loading data to the target
    targetHandler.writeFinalDF(complaintsReport, argConf.complaintsReportPath)
  } catch {
    case e: Throwable =>
      logger.warn(s"Something went wrong! Exception: ", e)
  } finally {
    spark.sparkContext.stop()
    spark.stop()
  }

  def createComplaintReportDF(complaintsSourceCleaned: DataFrame, categoryMap: DataFrame, serviceMap: DataFrame): DataFrame = {
    // Using left-outer join because we would still want to see the complaints on the report if category/service details are missing
    complaintsSourceCleaned
      .join(broadcast(categoryMap), complaintsSourceCleaned("category_id") === categoryMap("category_id"), "left_outer")
      .join(broadcast(serviceMap), complaintsSourceCleaned("service_id") === serviceMap("service_id"), "left_outer")
      .select(
        serviceMap("service_name"),
        categoryMap("category_name"),
        complaintsSourceCleaned("created_at").as("complaint_created_at"),
        complaintsSourceCleaned("text").as("complaint_text")
      )
  }
}

object ComplaintReport {
  def apply(argConf: ArgConf,
            targetHandler: TargetHandler,
            transformationHandler: TransformationHandler,
            sourceHandler: SourceHandler
           ): ComplaintReport = new ComplaintReport(argConf, targetHandler, transformationHandler, sourceHandler)
}
