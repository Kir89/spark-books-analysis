package it.kirsoft.examples.booksanalysisspark.jobs

import it.kirsoft.examples.booksanalysisspark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class LoadTest extends AnyFunSuite with PrivateMethodTester {

  val appConfig = new ApplicationConfig("application-test.conf", "load", "nothing")
  val spark = SparkSession
    .builder
    .master(appConfig.sparkConfig.master)
    .appName(appConfig.sparkConfig.appName)
    .getOrCreate()
  val job = new Load(appConfig, spark)
  val sQLContext = spark.sqlContext
  import sQLContext.implicits._

  val computeLogic = PrivateMethod[DataFrame]('computeLogic)

  test("Load.computeLogic") {

    //schema of input data
    //root
    // |-- book_authors: string (nullable = true)
    // |-- book_desc: string (nullable = true)
    // |-- book_edition: string (nullable = true)
    // |-- book_format: string (nullable = true)
    // |-- book_isbn: string (nullable = true)
    // |-- book_pages: string (nullable = true)
    // |-- book_rating: string (nullable = true)
    // |-- book_rating_count: string (nullable = true)
    // |-- book_review_count: string (nullable = true)
    // |-- book_title: string (nullable = true)
    // |-- genres: string (nullable = true)
    // |-- image_url: string (nullable = true)

    val input = Seq(
      ("author1", "desc1", "edition1", "format1", "isbn1", "pages1", "4.00", "10", "5", "title1", "genre1|genre2", "image1"),
      ("author2", "desc2", "edition2", "format2", "isbn2", "pages2", "3.00", "5", "1", "title2", "genre3|genre4", "image2"),
      ("author2", "desc2", "edition2", "format2", "isbn2", "pages2", "UNPARSABLERATING", "5", "1", "title2", "genre3|genre4", "image2"),
      ("author2", "desc2", "edition2", "format2", "isbn2", "pages2", "3.00", "UNPARSABLERATINGCOUNT", "1", "title2", "genre3|genre4", "image2"),
      ("author2", "desc2", "edition2", "format2", "isbn2", "pages2", "100.00", "5", "1", "title2", "genre3|genre4", "image2"),
      ("author2", "desc2", "edition2", "format2", "isbn2", "pages2", "3.00", "900000000", "1", "title2", "genre3|genre4", "image2"),
      ("author2", "desc3", "edition3", "format3", "isbn3", "pages3", "5.00", "10", "1", "  title2  ", "genre3|genre4", "image2")
    ).toDF("book_authors", "book_desc", "book_edition", "book_format", "book_isbn", "book_pages", "book_rating",
      "book_rating_count", "book_review_count", "book_title", "genres", "image_url")

    val result : Dataset[Row] = job.invokePrivate(computeLogic(input))
    result.show(false)

    assert(result.count == 2)
    val book_weight_rating: Array[Row] = result
      .select("book_weight_rating")
      .filter(col("book_title")
        .contains("title2"))
      .take(1)
    assert(book_weight_rating(0).getFloat(0) == 50.0)

    val topTitle: Row = result
      .select("book_title")
      .first
    assert(topTitle.getString(0) == "title2")

  }

}
