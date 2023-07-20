package org.example

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date}


object Task3 extends App{
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
    .withColumn("birthdate", to_date(col("birthdate"),"dd.MM.yyyy"))

  usersDf.show(5)
  usersDf.printSchema()

  // Generate a single CSV file with the list of users who are older than 18 years old
  // consider that you have the field birthdate and that the current date is 2020-01-01
  // users who born before 2002-01-01 are over 18 years old
  val usersOlderThan18Df = usersDf.filter(col("birthdate") <= to_date(lit("01.01.2002"), "dd.MM.yyyy"))

  usersOlderThan18Df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header",true)
    .csv("output/output_3.csv")


}
