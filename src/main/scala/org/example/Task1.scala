package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession

object Task1 extends App{
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

  // the top 10 movies by score.
  //In case of a draw order alphabetical by title using spark
  moviesDf.createOrReplaceTempView("movies")
  val top10MoviesDf = spark.sql("select * from movies order by Score desc, Title asc limit 10")

  top10MoviesDf.coalesce(1).write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_1.csv")


}
