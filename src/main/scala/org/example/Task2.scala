package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}


object Task2 extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();

  // read csv file located at input/mock_data_movies.csv
  val moviesDf = spark.read
    .option("header",true)
    .option("inferSchema", "true")
    .csv("input/mock_data_movies.csv")

  moviesDf.createOrReplaceTempView("movies")
  val moviesExplodedGenreDf = spark.sql("select id, price, Title, Score, explode(split(Genre, '[|]')) as Genre from movies")

  moviesExplodedGenreDf.createOrReplaceTempView("moviesExplodedGenre")

  val scoreDenseRankGenre = spark.sql("select *, dense_rank() over (partition by Genre order by Score desc, Title asc) as rank from moviesExplodedGenre")
  scoreDenseRankGenre.createOrReplaceTempView("scoreDenseRankGenre")
  val top3Df = spark.sql("select Genre, concat_ws('|', collect_list(id)) as top3 from scoreDenseRankGenre where rank <= 3 group by Genre")

  top3Df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_2.csv")


}
