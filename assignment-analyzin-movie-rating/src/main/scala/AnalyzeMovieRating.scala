import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object AnalyzeMovieRating extends App{

  // creating spark session
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("AnalyzeMovieRating")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  // Read ratings data from CSV file
  val df = try {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("assignment-analyzing-movie-rating/src/main/resources/ratings.csv")
  } catch {
    case e: Exception =>
      println(s"An error occurred while reading the ratings CSV file: ${e.getMessage}")
      spark.emptyDataFrame
  }

  // Calculate the mean rating for each movie
  def getMeanRatings(df: DataFrame): DataFrame = {
    df.groupBy("movieId")
      .agg(avg("rating").cast(DoubleType).alias("mean_rating"))
  }

  // Calculate the standard deviation of ratings for each movie
  def getStdDevRatings(df: DataFrame): DataFrame = {
    df.groupBy("movieId")
      .agg(stddev(coalesce(col("rating"), lit(0))).cast(DoubleType).alias("stddev_rating"))
  }

  // Join the mean and standard deviation ratings DataFrames
  def getMovieRatings(df: DataFrame): DataFrame = {
    getMeanRatings(df).join(getStdDevRatings(df), Seq("movieId"))
  }

  // Show the resulting DataFrame
  val movieRatings = try {
    getMovieRatings(df)
  } catch {
    case e: Exception =>
      println(s"An error occurred while calculating movie ratings: ${e.getMessage}")
      spark.emptyDataFrame
  }
  movieRatings.show()

}