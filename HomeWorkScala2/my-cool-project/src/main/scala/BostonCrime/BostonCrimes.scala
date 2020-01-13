package BostonCrime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Test(DISTRICT: String, crime_type: String, count: Long)

object BostonCrimes extends App {

  import org.apache.log4j.BasicConfigurator

  BasicConfigurator.configure()

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val csvFolder1 = args(0)
  val csvFoler2 = args(1)
  val parqPath = args(2)

  val crime = spark.read.format("csv").option("header", "true").load(s"$csvFolder1/crime.csv")
  val offence_codes = spark.read.format("csv").option("header", "true").load(s"$csvFoler2/offense_codes.csv")

  val OffenceCodesBroadcast = broadcast(offence_codes)

  val df = crime
    .join(OffenceCodesBroadcast, crime("OFFENSE_CODE").cast("Long") === OffenceCodesBroadcast("CODE"))
    .select(coalesce($"DISTRICT", lit("Не определен")).as("DISTRICT"), $"Lat", $"Long", substring_index(col("NAME"), " - ", 1).as("crime_type"), $"MONTH")

  val df_top = df
    .groupBy("DISTRICT", "crime_type")
    .count()
    .as[Test]
    .groupByKey(x => x.DISTRICT)
    .flatMapGroups {
      case (district, iter) => iter.toList.sortBy(x => -x.count).take(3)
  }
    .groupBy("DISTRICT")
    .agg(concat_ws(", ", collect_list("crime_type")).as("crime_type"))

  val df_count_avg = df
    .groupBy("DISTRICT")
    .agg(avg("Lat").as("avg_lat"), avg("Long").as("avg_long"), count("DISTRICT").as("count_crime"))

  val df_count_by_month = df
      .groupBy("DISTRICT", "MONTH")
      .count()

  df_count_by_month.createOrReplaceTempView("df")

  val df_med = spark.sql("SELECT DISTRICT, percentile_approx(count, 0.5) as medium FROM df GROUP BY DISTRICT")

  df_top.alias("top")
    .join(df_count_avg.alias("count_avg"), Seq("DISTRICT"))
    .join(df_med.alias("med"), Seq("DISTRICT"))
    .repartition(1)
    .write
    .parquet(s"$parqPath/output.parquet")
}