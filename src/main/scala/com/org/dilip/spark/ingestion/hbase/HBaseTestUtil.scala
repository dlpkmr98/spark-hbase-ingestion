package com.org.dilip.spark.ingestion.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.conf.Configuration

trait HBaseTestUtil {

  lazy val hbaseTestingUtility: HBaseTestingUtility = {
    val htu = HBaseTestingUtility.createLocalHTU()
    htu.getConfiguration().set("test.hbase.zookeeper.property.clientPort", "2181")
    htu
  }
  lazy implicit val conf: Configuration = HBaseConfig(hbaseTestingUtility.getConfiguration) get

  /**
   * @param event start|stop
   */
  def toHbaseMiniCluster(event: String): Unit = event match {
    case "Start" | "start" => {
      System.setProperty("test.build.data.basedirectory", "/tmp")
      hbaseTestingUtility.cleanupTestDir()
      println("Starting Mini Cluster...........")
      hbaseTestingUtility.startMiniCluster()
      println("Mini Cluster started................")
    }

    case "Stop" | "stop" => {
      println("Shutting down Mini Cluster..............")
      hbaseTestingUtility.shutdownMiniCluster()
      println("Mini Cluster Shut Down...........")
      hbaseTestingUtility.cleanupTestDir()
    }
    case _ => println("required input==> start | stop")
  }
}