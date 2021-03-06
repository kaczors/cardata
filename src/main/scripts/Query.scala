import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

spark.read.json("/home/admin/data.json").write.parquet("/home/admin/data.parquet")
val cars = spark.read.load("/home/admin/data.parquet")

def calculate4BucketsFlat(percentRank: Column): Column = {
  when(percentRank >= 0.75, 1.1)
    .when(percentRank > 0.5, 1)
    .when(percentRank > 0.25, 0.9)
    .otherwise(0.8)
}

def calculate4BucketsLast0(percentRank: Column): Column = {
  when(percentRank >= 0.75, 1.5)
    .when(percentRank > 0.5, 1.25)
    .when(percentRank > 0.25, 1)
    .otherwise(0)
}

def calculate4Buckets(percentRank: Column): Column = {
  when(percentRank >= 0.75, 1.5)
    .when(percentRank > 0.5, 1.25)
    .when(percentRank > 0.25, 1)
    .otherwise(0.75)
}


val result = cars
  .where(!col("name").contains(lit("Renault Thalia")))
  .where(!col("name").contains(lit("Dacia Logan")))
  .where(!col("name").contains(lit("Nissan Note")))
  .where(!col("name").contains(lit("Sedan")))
  .where(!col("name").contains(lit("Kombi")))
  .where(!col("name").contains(lit("Tourer")))
  .where(!col("name").contains(lit("Wagon")))
  .where(!col("name").contains(lit("Variant")))
  .where(col("seats") === 5)
  .where(col("doors") >= 5)
  .where(col("fuel") === "benzynowy")
  .where(col("cc") <= 1850L)
  .where(col("acc") < 12.0)
  .where(col("fuel-avg").isNotNull)
  .where(col("trunk").isNotNull && col("trunk") >= 385)
  .where(col("rating").isNotNull)
  .where(col("rating") >= 4)
  .where(col("weight") <= 1400)
  .where(col("hp") < 150 && col("hp") > 80)
  .where(col("year-from") >= 2008 && col("year-from") <= 2013)
  .where(col("rating-count") >= 5)
  .where(col("fuel-reports-count") >= 5)
  .where(col("length") >= 3850 && col("length") <= 4500)
  .where(col("vmax") >= 180)
  .withColumn("acc_s", calculate4Buckets(percent_rank().over(orderBy(col("acc").desc))))
  .withColumn("cc_s", calculate4Buckets(percent_rank().over(orderBy(col("cc").desc))))
  .withColumn("fuel-avg_s", calculate4Buckets(percent_rank().over(orderBy(col("fuel-avg").desc))))
  .withColumn("fuel-reports-count_s", calculate4BucketsFlat(percent_rank().over(orderBy(col("fuel-reports-count").asc))))
  .withColumn("trunk_s", calculate4BucketsLast0(percent_rank().over(orderBy(col("trunk").asc))))
  .withColumn("rating_s", calculate4Buckets(percent_rank().over(orderBy(col("rating").asc))))
  .withColumn("rating-count_s", calculate4BucketsFlat(percent_rank().over(orderBy(col("rating-count").asc))))
  .withColumn("weight_s", calculate4Buckets(percent_rank().over(orderBy(col("weight").desc))))
  .withColumn("vmax_s", calculate4Buckets(percent_rank().over(orderBy(col("vmax").asc))))
  .withColumn("year-from_s", calculate4Buckets(percent_rank().over(orderBy(col("year-from").desc))))
  .withColumn("score", col("year-from_s") + col("weight_s") + col("acc_s") + col("cc_s") + lit(3).multiply(col("fuel-avg_s")).multiply(col("fuel-reports-count_s")) + lit(1).multiply(col("trunk_s")) + lit(3.5).multiply(col("rating_s")).multiply(col("rating-count_s")))
  .withColumn("no", row_number().over(Window.orderBy(desc("score"))))
  .orderBy(desc("score"))


result.select(col("no"), col("name"), format_number(col("score"), 2).as("score"), col("year-from").as("from"), col("year-to").as("to"), col("cc"), col("acc"), col("vmax"), col("hp"), col("weight"), col("trunk"), col("length"), concat(col("fuel-avg"), lit("("), col("fuel-reports-count"), lit(")")).as("fuel-avg"),concat(col("lpg-fuel-avg"), lit("("), col("lpg-reports-count"), lit(")")).as("lpg"), concat(col("rating"), lit("("), col("rating-count"), lit(")")).as("rating"), col("doors")).show(100, false)
