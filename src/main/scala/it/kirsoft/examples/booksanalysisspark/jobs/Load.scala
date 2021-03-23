package it.kirsoft.examples.booksanalysisspark.jobs

import it.kirsoft.examples.booksanalysisspark.config.ApplicationConfig
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max, row_number, trim}
import org.apache.spark.sql.types.{FloatType, IntegerType}

import scala.util.Try

/**
 * This class implements the job that starting from the csv of the books, loads the ranked data on a DB configured with
 * jobConfig
 * @param jobConfig contains the job configuration, with details about Spark and the connection to the DB
 * @param spark the Spark Session
 */
class Load(val jobConfig: ApplicationConfig, val spark: SparkSession) extends GenericJob {

  val MAX_RATINGS_COUNT = 500000000

  val inputPath = jobConfig.inputFile

  /**
   * Runs the job logic
   * @return a Try instance with the result of the computation
   */
  def run(): Try[Unit] = Try{

    System.out.println(jobConfig.sparkConfig.appName + " started...")

    //Input reading
    val booksInputDataDF: DataFrame = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("escape", """"""")
      .option("header", "true")
      .load(inputPath)

    val outputToDB: Dataset[Row] = computeLogic(booksInputDataDF)

    //Write
    outputToDB
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", "jdbc:mariadb://localhost:3306/books_example")
      .option("dbtable", "ranking")
      .option("user", "ricky")
      .option("password", "BooksTest!!")
      .save()

    System.out.println(jobConfig.sparkConfig.appName + " finished.")

  }

  /**
   * Contains compute logic of the job
   * @param booksInputDataDF input data read from file
   * @return result dataset, ready to be written to DB
   */
  private def computeLogic(booksInputDataDF: DataFrame): Dataset[Row] = {
    //Casting rating and rating count to make computations on them
    val castedDF: DataFrame = booksInputDataDF
      .withColumn("book_rating_casted", col("book_rating").cast(FloatType))
      .withColumn("book_rating_count_casted", col("book_rating_count").cast(IntegerType))

    //Computing casted data with applied outliers filtering conditions and cleaning book titles
    val cleanCastedBooksDataDF: DataFrame = castedDF
      .filter(col("book_rating_casted").isNotNull
        and col("book_rating_casted") <= 5
        and col("book_rating_casted") >= 0)
      .filter(col("book_rating_count_casted").isNotNull
        and col("book_rating_count_casted") < MAX_RATINGS_COUNT)
      .withColumn("book_title_cleaned", trim(col("book_title")))

    //Computing book_weight_rating
    val computedBookWeightRatingDF: DataFrame = cleanCastedBooksDataDF
      .select("book_title_cleaned", "book_desc", "book_authors", "genres", "image_url",
        "book_rating_casted", "book_rating_count_casted")
      .groupBy("book_title_cleaned")
      .agg(max("book_desc"), max("book_authors"), max("genres"),
        max("image_url"), max("book_rating_casted"),
        max("book_rating_count_casted"))
      .withColumn("book_weight_rating",
        col("max(book_rating_casted)") * col("max(book_rating_count_casted)"))

    //Ranking
    val window: WindowSpec = Window.orderBy(col("book_weight_rating").desc)
    val outputWithRankDF: DataFrame = computedBookWeightRatingDF
      .withColumn("rank", row_number.over(window))
      .withColumnRenamed("book_title_cleaned", "book_title")
      .withColumnRenamed("max(book_desc)", "book_desc")
      .withColumnRenamed("max(book_authors)", "book_authors")
      .withColumnRenamed("max(genres)", "genres")
      .withColumnRenamed("max(image_url)", "image_url")
      .withColumnRenamed("max(book_rating_casted)", "book_rating")
      .withColumnRenamed("max(book_rating_count_casted)", "book_rating_count")

    //Preparing data to be written to DB
    val outputToDB: Dataset[Row] = outputWithRankDF.orderBy("rank").
      select("book_title", "book_weight_rating", "book_desc", "book_authors", "genres", "image_url",
        "book_rating", "book_rating_count").limit(1000)

    outputToDB
  }
}
