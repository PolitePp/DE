package org.example

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.{JsonMethods, Serialization}


object JsonReader extends App {

  implicit val formats = DefaultFormats

  val spark = SparkSession.builder().master(master = "local").getOrCreate()

  val sc = spark.sparkContext

  val filename = args(0)

  val res = sc.textFile(filename)

  case class JsRow (
                     id: Option[Long]
                     , country: Option[String]
                     , points: Option[Long]
                     , price: Option[Number]
                     , title: Option[String]
                     , variety: Option[String]
                     , winery: Option[String]
                   )

  res.map{row => val json_row = parse(row)
    json_row.extract[JsRow]
  }.foreach{println _}
}
