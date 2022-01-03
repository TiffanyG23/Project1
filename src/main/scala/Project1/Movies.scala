package Project1
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

object Movies {
  def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    suppressLogs(List("org", "akka"))
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    val dfPopularMovies = spark.read.text("""C:/MoviesInput/MostPopularMovies.txt""")
    dfPopularMovies.printSchema()
    dfPopularMovies.show()
    val schema = new StructType()
      .add("id", StringType, true)
      .add("rank", StringType, true)
      .add("rankUpDown", StringType,true)
      .add("title", StringType, true)
      .add("fullTitle", StringType, true)
      .add("year", StringType, true)
      .add("image", StringType, true)
      .add("crew", StringType, true)
      .add("imDbRating", StringType, true)
      .add("imDbRatingCount", StringType, true)
    val dfJSON = dfPopularMovies.withColumn("movieData", from_json(col("value"),
      schema)).select("movieData.*")
    dfJSON.printSchema()
    dfJSON.write
      .mode("overwrite")
      .saveAsTable("movies")
    val dfTopMovies = spark.read.text("""C:/MoviesInput/Top250Movies.txt""")
    dfTopMovies.printSchema()
    dfTopMovies.show()
    val topSchema = new StructType()
      .add("id", StringType, true)
      .add("rank", StringType, true)
      .add("title", StringType, true)
      .add("fullTitle", StringType, true)
      .add("year", StringType, true)
      .add("image", StringType, true)
      .add("crew", StringType, true)
      .add("imDbRating", StringType, true)
      .add("imDbRatingCount", StringType, true)
    val dfTopJSON = dfTopMovies.withColumn("topMovieData", from_json(col("value"),
      topSchema)).select("topMovieData.*")
    dfTopJSON.printSchema()
    dfTopJSON.write
      .mode("overwrite")
      .saveAsTable("topMovies")
//      spark.sql("SELECT * FROM topMovies").show(250)
//    spark.sql("SELECT * FROM movies").show(102)
    spark.sql("SELECT * FROM Movies WHERE year = '2019' ORDER BY rank").show()
    spark.sql("SELECT * FROM topMovies WHERE crew = '&Brad Pitt&' ORDER BY rank").show()
    spark.close()
  }
}