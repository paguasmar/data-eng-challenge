package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp, when}


object Task6 extends App{
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

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  // read csv file located at input/mock_data_users.csv
  val usersDf = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv("input/mock_data_users.csv")
    .withColumn("birthdate", to_date(col("birthdate"), "dd.MM.yyyy"))

  val moviesDf = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv("input/mock_data_movies.csv")


  // users who born before 2002-01-01 are over 18 years old
  val usersOlderThan18Df = usersDf.filter(col("birthdate") <= to_date(lit("01.01.2002"), "dd.MM.yyyy"))

  // filter movies in moviesDf that do not contains horror in Genre field
  val moviesForYoungerThan18Df = moviesDf.filter(!col("Genre").contains("Horror"))

  // moviesForYoungerThan18Df select("id") convert it to array column
  val moviesForYoungerThan18DfArray = moviesForYoungerThan18Df.select("id").collect().map(_(0)).toArray
  val moviesForOlderThan18DfArray = moviesDf.select("id").collect().map(_(0)).toArray

  val usersAvailableMoviesDf = usersDf.withColumn("availbale_movies",
    when(col("birthdate") <= to_date(lit("01.01.2002"), "dd.MM.yyyy"), moviesForOlderThan18DfArray)
      .otherwise(moviesForYoungerThan18DfArray))

  usersAvailableMoviesDf.show(5)

  usersAvailableMoviesDf.coalesce(1)
    .write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_6.csv")


}
