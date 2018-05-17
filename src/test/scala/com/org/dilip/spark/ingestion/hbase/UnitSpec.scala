package com.org.dilip.spark.ingestion.hbase

import org.scalatest._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import com.org.dilip.spark.ingestion.spark.SparkSupport
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.util.Bytes

/**
 * Abstract base classes that mix together the features which use the most in project
 */
abstract class UnitSpec extends FlatSpec with Matchers with OptionValues with Inside 

trait UnitSpecTrait extends SuiteMixin with BeforeAndAfterAll {
  this: Suite =>

  // Sharing immutable fixture objects via instance variables
  implicit val sqlCtx = SparkSupport.sqlCtx
  implicit val sc = SparkSupport.sc
  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def arrayToBytes(a: Array[String]): Array[Array[Byte]] = a map Bytes.toBytes
  import sqlCtx.implicits._
  
  override def beforeAll {
    setSparkContextParameter
  }

  override def afterAll {
    sqlCtx.clearCache()
  }

  def setSparkContextParameter = {
    sqlCtx.setConf("spark.sql.broadcastTimeout", "1200")
    sqlCtx.setConf("spark.default.parallelism", "10")
    sqlCtx.setConf("spark.sql.shuffle.partitions", "10")
    sqlCtx.setConf("spark.shuffle.consolidateFiles", "true")
    sqlCtx.setConf("spark.driver.extraJavaOptions", "-Xmx5g")
    Logger.getRootLogger().setLevel(Level.ERROR)

  }

  val testDF: DataFrame = {
    Seq((1, "value2", "value3", "value4"), (11, "value22", "value33", "value44")).toDF("c1", "c2", "c3", "c4")
  }

  val header = Seq("origin_gvg", "origin_country", "origin_city", "origin_airport",
    "destination_gvg", "destination_country", "destination_city", "destination_airport")
  val rows = Array(
    Array("NA", "US", "NYC", "EWR", "EU", "DE", "FRA", "FRA"),
    Array("origin_gvg_value", "origin_country_value", "origin_city_value", "OG",
      "destination_gvg_value", "destination_country_value", "DC",
      "destination_airport_value"))

}