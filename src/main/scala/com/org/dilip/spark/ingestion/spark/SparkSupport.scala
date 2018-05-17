package com.org.dilip.spark.ingestion.spark

import org.apache.spark.SparkConf
import com.org.dilip.spark.ingestion.config.ConfigLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkSupport {
  
  val conf = new SparkConf().setMaster(ConfigLoader.master).setAppName(ConfigLoader.appName)
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlCtx = SQLContext.getOrCreate(sc)

}