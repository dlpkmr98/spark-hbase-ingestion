package com.org.dilip.spark.ingestion.config

import com.typesafe.config.ConfigFactory

object ConfigLoader {
  
  lazy val appConf = ConfigFactory.load()
  lazy val hbaseConf = ConfigFactory.load("hbase")
  lazy val appName = appConf.getString("application.name")
  lazy val master = appConf.getString("application.master")
  lazy val hadoopCoreXmlPath = hbaseConf.getString("hbase.hadoopCoreXmlPath")
  lazy val hbaseXmlPath = hbaseConf.getString("hbase.hbaseXmlPath")
  lazy val tableName = hbaseConf.getString("hbase.table_name")
  lazy val col_family_name = hbaseConf.getString("hbase.col_family_name")
  
}