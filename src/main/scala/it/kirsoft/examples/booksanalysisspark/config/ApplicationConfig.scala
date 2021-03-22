package it.kirsoft.examples.booksanalysisspark.config

import com.typesafe.config._

/**
 * Class used to read configuration
 * @param configFile Name of the file to read as configuration
 */
class ApplicationConfig(val configFile: String, val jobName: String, val inputFile: String) {

  val config: Config = ConfigFactory.load(configFile).getConfig("it.kirsoft.examples.booksanalysisspark")

  val sparkConfig = SparkConfig(
    config.getConfig("spark").getString("app-name"),
    config.getConfig("spark").getString("master"),
    config.getConfig("spark").getString("log-level")
    )

  val dbConfig = DBConfig(
    config.getConfig("db").getString("url"),
    config.getConfig("db").getString("dbtable"),
    config.getConfig("db").getString("username"),
    config.getConfig("db").getString("password")
  )

}
