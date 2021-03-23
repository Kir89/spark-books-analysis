package it.kirsoft.examples.booksanalysisspark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import it.kirsoft.examples.booksanalysisspark.config.{ApplicationConfig, ConfigCreationException}
import it.kirsoft.examples.booksanalysisspark.jobs._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object Main {

  def main(args: Array[String]) {

    val triedAppConfig: Try[ApplicationConfig] = Try(createConfig(args))
    val appConfig: ApplicationConfig = triedAppConfig match {
      case Success(value) => value
      case Failure(exception) => {
        System.out.print(exception.getMessage)
        exception.printStackTrace()
        exception match {
          case InputParamsException(_) => System.exit(1); null
          case ConfigCreationException(_, _) => System.exit(2); null //null avoids the compiler to complain :)
        }
      }
    }

    val spark = SparkSession
      .builder
      .master(appConfig.sparkConfig.master)
      .appName(appConfig.sparkConfig.appName)
      .getOrCreate()
    spark.sparkContext.setLogLevel(appConfig.sparkConfig.logLevel)

    //Running the job chosen by user
    appConfig.jobName match {
      case "load" => runner(new Load(appConfig, spark))
      case "retrieve" => runner(new Retrieve(appConfig, spark))
    }

    spark.stop()

  }

  /**
   * Runs the specified job
   * @param job the job you need to run
   */
  def runner(job: GenericJob) = {
    val result: Try[Unit] = job.run()
    result match {
      case Success(_) => System.out.println("Job executed correctly")
      case Failure(exception) => {
        System.out.println("Error during job execution")
        System.out.println(exception.getMessage)
        exception.printStackTrace()
        System.exit(3)
      }
    }
  }

  /** TODO use this function for defining the log file name!
   * Defines the name of the log file based on current date and time
   * @return the name of the log file for the current run
   */
  def logFile(): String = {
    val currentDate: Date = Calendar.getInstance.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd_hh:mm:ss")
    val currentDatePrintable: String = dateFormat.format(currentDate)

    "./booksanalysis_" + currentDatePrintable +".log"
  }

  /**
   * This function checks the arguments of the application and if ok, returns the application configuration
   * 1st param: name of config file to use
   * 2nd param: job to run. load|retrieve
   * 3rd param: input file path (only needed and used if "load" job executed)
   * @param args arguments of the application
   * @return instance of ApplicationConfig with the configuration for the application
   */
  def createConfig(args: Array[String]): ApplicationConfig = {
    if(args.length < 2 //Not enough params
      || (args.length == 2 && args(1) == "load") //load needs the third param, to reach the input dataset
      || (args.length >= 2 && (args(1) != "load" && args(1) != "retrieve")) //matching only the correct job names
      || (args.length > 3)) //Avoid more than necessary params
    {
      throw InputParamsException("Usage:\n1st param: name of config file to use\n2nd param: job to run. load|retrieve\n3rd param:" +
        " input file path (only needed and used if \"load\" job executed)")
    } else {
      val config = Try(new ApplicationConfig(args(0), args(1), Try(args(2)).getOrElse("")))
      config match {
        case Success(value) => value
        case Failure(exception) => throw ConfigCreationException("Error while creating job configuration\n" + exception.getMessage, exception)
      }
    }
  }

}
