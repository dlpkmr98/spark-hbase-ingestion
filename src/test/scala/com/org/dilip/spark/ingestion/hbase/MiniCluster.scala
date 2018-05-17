package com.org.dilip.spark.ingestion.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, Suite, SuiteMixin }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseCluster
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.LocalHBaseCluster
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import com.org.dilip.spark.ingestion.config.ConfigLoader

trait MiniCluster extends SuiteMixin with BeforeAndAfterAll with UnitSpecTrait with HBaseTestUtil { this: Suite =>

  val tableName = ConfigLoader.tableName
  val cf = ConfigLoader.col_family_name

  override def beforeAll() {
    setSparkContextParameter
    toHbaseMiniCluster("start")
  }

  override def afterAll() {
    toHbaseMiniCluster("stop")
  }
}