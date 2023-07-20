package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp, when}


object Task4 extends App{
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
    .option("header",true)
    .option("inferSchema", "true")
    .csv("input/mock_data_users.csv")
    .withColumn("birthdate", to_timestamp(col("birthdate"),"dd.MM.yyyy"))

  // Write a single Parquet file with the same data contained in the “mock_data_users.csv” file.
  //But split the value of the money and the currency to different columns.
  //For example, if the value of the money is “$100”, the value of the column money should be 100 and the value of the column currency should be “USD”.

  val usersDf2 = usersDf.withColumn("currency", col("money").substr(1, 1))
  val usersDf3 = usersDf2.withColumn("money", col("money").substr(2, 100).cast("double"))

  // map column currency to USD if $, EUR if €, GBP if £
  val usersDf4 = usersDf3
    .withColumn("currency",
      when(col("currency") === "$", "USD")
      .when(col("currency") === "€", "EUR")
      .when(col("currency") === "£", "GBP")
      .otherwise("Unknown"))

  usersDf4.printSchema()

  usersDf4.coalesce(1)
    .write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_4.csv")


}
