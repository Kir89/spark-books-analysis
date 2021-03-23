package it.kirsoft.examples.booksanalysisspark.jobs

import it.kirsoft.examples.booksanalysisspark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class RetrieveTeste extends  AnyFunSuite with PrivateMethodTester {

  val appConfig = new ApplicationConfig("application-test.conf", "retrieve", "nothing")
  val spark = SparkSession
    .builder
    .master(appConfig.sparkConfig.master)
    .appName(appConfig.sparkConfig.appName)
    .getOrCreate()
  val job = new Retrieve(appConfig, spark)
  val sQLContext = spark.sqlContext

  val computeLogic = PrivateMethod[DataFrame]('computeLogic)

  test("Retrieve.computeLogic") {

    //Schema of input data
    //root
    // |-- book_title: string (nullable = true)
    // |-- book_weight_rating: double (nullable = true)
    // |-- book_desc: string (nullable = true)
    // |-- book_authors: string (nullable = true)
    // |-- genres: string (nullable = true)
    // |-- image_url: string (nullable = true)
    // |-- book_rating: double (nullable = true)
    // |-- book_rating_count: integer (nullable = true)

    val input = Seq(
      Row("title1", 100.5, "desc1", "auth1", "genre1|genre2", "image1", 4.0, 18),
      Row("title2", 50.5, "desc2", "auth2", "genre2|genre3", "image1", 4.0, 18)
    )
    val schema = List(
      StructField("book_title", StringType, true),
      StructField("book_weight_rating", DoubleType, true),
      StructField("book_desc", StringType, true),
      StructField("book_authors", StringType, true),
      StructField("genres", StringType, true),
      StructField("image_url", StringType, true),
      StructField("book_rating", DoubleType, true),
      StructField("book_rating_count", IntegerType, true)
    )

    val inputData = spark.createDataFrame(
      spark.sparkContext.parallelize(input),
      StructType(schema)
    )

    val result : Dataset[Row] = job.invokePrivate(computeLogic(inputData))

    result.show(false)

    assert(result.count == 3)
    assert(result.first.getString(0) == "genre2")
    assert(result.first.getDouble(1) == 151)
  }

}
