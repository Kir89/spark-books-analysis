package it.kirsoft.examples.booksanalysisspark.config

/**
 * Contains the Spark App Configuration
 * @param appName Name of Spark Application
 * @param master Spark Master
 * @param logLevel logging level for Spark Application
 */
case class SparkConfig(appName: String, master: String, logLevel: String)
