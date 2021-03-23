package it.kirsoft.examples.booksanalysisspark.jobs

import it.kirsoft.examples.booksanalysisspark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_distinct, col, explode, split, sum}

import scala.util.Try

/**
 * Retrieves top 10 genres from data stored in the DB
 * @param jobConfig contains the job configuration, with details about Spark and the connection to the DB
 * @param spark the Spark Session
 */
class Retrieve(val jobConfig: ApplicationConfig, val spark: SparkSession) extends GenericJob {

  /**
   * Runs the job logic
   * @return a Try instance with the result of the computation
   */
  override def run(): Try[Unit] = Try {

    System.out.println(jobConfig.sparkConfig.appName + " started...")

    //Read data
    val jdbcDF: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", jobConfig.dbConfig.url)
      .option("dbtable", jobConfig.dbConfig.dbTable)
      .option("user", jobConfig.dbConfig.username)
      .option("password", jobConfig.dbConfig.password)
      .load()

    computeLogic(jdbcDF).show

    System.out.println(jobConfig.sparkConfig.appName + " finished.")

  }

  /**
   * Contains compute logic of the job
   * @param jdbcDF input data read from DB (using JDBC)
   * @return the result dataset to be printed
   */
  private def computeLogic(jdbcDF: DataFrame): Dataset[Row] = {
    //Create table with genres and book_weight_rating. Start to split genres in single elements
    val genresDF: DataFrame = jdbcDF
      .select("genres", "book_weight_rating")
      .withColumn("genresArray", split(col("genres"), "\\|"))

    //Explode the array of genres after taking the distinct values. Keep book weight rating because it will be used to
    //compute the genre rating
    val explodedGenresDF: DataFrame = genresDF
      .select(
        explode(
          array_distinct(col("genresArray"))).as("genre"), col("book_weight_rating"))

    //Aggregating by equal genre and summing up all the book_weight_rating to compute genre_weight_rating, and sorting
    val genreRanking: Dataset[Row] = explodedGenresDF
      .groupBy("genre")
      .agg(sum(col("book_weight_rating"))
        .as("genre_weight_rating"))
      .orderBy(col("genre_weight_rating").desc)
      .limit(10)

    genreRanking
  }


}
