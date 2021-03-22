package it.kirsoft.examples.booksanalysisspark.jobs

import it.kirsoft.examples.booksanalysisspark.config.ApplicationConfig
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * This trait represents a job in a generic way, with its attributes and a core method
 */
trait GenericJob {
  val jobConfig: ApplicationConfig
  val spark: SparkSession
  def run(): Try[Unit]
}
