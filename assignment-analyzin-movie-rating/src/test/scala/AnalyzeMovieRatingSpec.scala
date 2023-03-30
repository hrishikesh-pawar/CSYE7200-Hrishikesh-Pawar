import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class AnalyzeMovieRatingSpec extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("AnalyzeMovieRatingSpec")
    .config("spark.ui.enabled", false)
    .getOrCreate()

  import spark.implicits._

  private var testDF: DataFrame = _

  override def beforeEach(): Unit = {
    testDF = Seq(
      (1, 4.0),
      (1, 3.0),
      (1, 5.0),
      (2, 2.0),
      (2, 1.0),
      (2, 3.0)
    ).toDF("movieId", "rating")
  }

  test("meanRatings should be calculated correctly") {
    val meanRatings = AnalyzeMovieRating.getMeanRatings(testDF)
    assert(meanRatings.collect() === Seq(
      Row(1, 4.0),
      Row(2, 2.0)
    ))
  }

  test("stdDevRatings should be calculated correctly") {
    val stdDevRatings = AnalyzeMovieRating.getStdDevRatings(testDF)
    assert(stdDevRatings.collect() === Seq(
      Row(1, 1.0),
      Row(2, 1.0)
    ))
  }

  test("movieRatings should be joined correctly") {
    val movieRatings = AnalyzeMovieRating.getMovieRatings(testDF)
    assert(movieRatings.collect() === Seq(
      Row(1, 4.0, 1.0),
      Row(2, 2.0, 1.0)
    ))
  }

  override def afterAll(): Unit = {
    spark.catalog.clearCache()
    spark.stop()
  }

}
