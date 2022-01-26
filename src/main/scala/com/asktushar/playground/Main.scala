package com.asktushar.playground

import com.asktushar.playground.conf.ArgConf
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}
import com.asktushar.playground.jobs.{ComplaintReport, TransformationHandler}
import com.asktushar.playground.utils.{SourceHandler, TargetHandler}

object Main extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def run(): Try[Unit] = Try[Unit] {
    val start = System.currentTimeMillis()
    logger.info("Starting the job at: " + start)

    if (args.length != 6) {
      logger.warn(s"Arguments passed are not correct, Please check: " + args.foreach(println))
      sys.exit(1)
    }

    //TODO Use ScallopConf instead
    val conf = ArgConf(args(0), args(1), args(2), args(3), args(4), args(5))

    conf.taskType match {
      case "ComplaintReport" => ComplaintReport(conf, new TargetHandler, new TransformationHandler, new SourceHandler)
        .generateReport()
    }

    val took = System.currentTimeMillis() - start
    logger.info(conf.taskType + "_core_duration", took)
  }

  run() match {
    case Success(_) =>
      logger.info("Finished the job successfully. Goodbye")
    case Failure(exception) =>
      logger.warn(s"Something went wrong! Exception: ", exception)
      sys.exit(1)
  }


}
