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

  // На входе путь до папки, где хранятся csv и куда будет писать паркет
  val csvFolder1 = args(0)

  // Прочитали источники данных
  val crime = spark.read.format("csv").option("header", "true").load(s"$csvFolder1/crime.csv")
  val offence_codes = spark.read.format("csv").option("header", "true").load(s"$csvFolder1/offense_codes.csv").groupBy("CODE", "NAME").count()

  // Закэшировали словарь с кодами
  val OffenceCodesBroadcast = broadcast(offence_codes)

  // Сделали представления на основе источников для работы со SparkSQL

  crime.createOrReplaceTempView("crime")
  OffenceCodesBroadcast.createOrReplaceTempView("offence_codes")

  // Получаем общее кол-во преступлений по району и среднюю долготу и ширину (для джоина используем оба поля + выбираем уникальные значения)

  val df_common = spark.sql(
    "SELECT coalesce(district, 'Not defined') as district, count(*) as crimes_total, avg(Lat) as lat, avg(Long) as lng " +
    "FROM crime c " +
    "inner join " +
    "(select distinct code, name from offence_codes) oc " +
    "on cast(c.offense_code as bigint) = code " +
    "and c.offense_description = oc.name " +
    "group by coalesce(district, 'Not defined') ")

  // Находим медиану кол-ва преступлений по месяцам в разрезе районов

  val df_by_month = spark.sql(
    "SELECT district, percentile_approx(medium, 0.5) as crimes_monthly " +
      "from (" +
      "select coalesce(district, 'Not defined') as district, month, count(*) as medium " +
      "from crime " +
      "group by coalesce(district, 'Not defined'), month, year " +
      ") s1 group by district"
  )

  // Находим топ3 типов преступлений в разрезе района

  val df_by_crime_type = spark.sql(
    "SELECT district, concat_ws(', ', collect_list(crime_type)) as frequent_crime_types " +
      "from (" +
      "SELECT coalesce(district, 'Not defined') as district, split(offense_description, ' - ')[0] as crime_type, count(*) as cn, " +
      "row_number() over (partition by coalesce(district, 'Not defined') order by count(1) desc) as rn " +
      "from crime " +
      "group by coalesce(district, 'Not defined'), split(offense_description, ' - ')[0] " +
      ") s1 where rn < 4 " +
      "group by district"
  )

  // Соединяем результаты и записываем в паркет

  df_common
    .join(df_by_month, Seq("district"))
    .join(df_by_crime_type, Seq("district"))
    .repartition(1)
    .write
    .parquet(s"$csvFolder1/output.parquet")
}