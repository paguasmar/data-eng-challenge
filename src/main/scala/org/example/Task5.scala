package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, floor, regexp_replace, to_timestamp, when}


object Task5 extends App{
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
  val usersCurrencyDf = spark.read
    .option("header",true)
    .option("inferSchema", "true")
    .csv("output/output_4.csv")

  val moviesDf = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv("input/mock_data_movies.csv")

  // map column currency to USD if $, EUR if €, GBP if £
  val usersDf4 = usersCurrencyDf
    .withColumn("tmp_money_euros",
      when(col("currency") === "USD", col("money") * 0.90)
      .otherwise(col("money")))

  // round to 2 decimals with floor to make sure they have sufficient money
  val usersDf5 = usersDf4
    .withColumn("money_euros", floor(col("tmp_money_euros")*100)/100)

  usersDf5.printSchema()
  usersDf5.show(5)

  // calculate the total price of all movies
  val moviesPriceDouble = moviesDf.withColumn("price_double", regexp_replace(col("price").substr(2, 100), ",", ".").cast("double"))
  moviesPriceDouble.show(5)
  val priceBuyingAllMovies = moviesPriceDouble.select("price_double").rdd.map(r => r(0).asInstanceOf[Double]).sum()

  val usersCanBuyAllMovies = usersDf5.filter(col("money_euros") >= priceBuyingAllMovies)

  usersCanBuyAllMovies.coalesce(1)
    .write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_5.csv")

}
