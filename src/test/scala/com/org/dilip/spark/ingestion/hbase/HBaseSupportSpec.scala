package com.org.dilip.spark.ingestion.hbase


import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.BeforeAndAfter
import org.scalatest.ParallelTestExecution



class HBaseSupportSpec extends UnitSpec with MiniCluster with BeforeAndAfter with Serializable  {

  before {
    try {
      hbaseTestingUtility.deleteTable(tableName)
    } catch {
      case e: Exception => {
        println("No Table " + tableName + " found")
      }
    }
    println("Creating HBase Table " + tableName + "," + cf)
    try {
      hbaseTestingUtility.createTable(tableName, cf)
      println("HBase Table Created.................")
    } catch {
      case e: Exception => println("Error Creating Table:" + e.getMessage)
    }
  }

  after {
    hbaseTestingUtility.deleteTable(tableName)
    println("HBase Table Deleted")
  }

  "A rdd" should "read/write to a table with one column family" ignore {

    val keys_index = Array(3, 6)
    val sep = "##"
    //convert to hbase records
    val rdd = HBaseSupport.toDFToHbaseRDD(header, rows, keys_index, sep)(cf)

    //bulkput
    HBaseSupport.toHbaseBulkPutRDD(tableName, rdd)
    val rdd_key = sc.parallelize(Array((Bytes.toBytes("EWR" + sep + "FRA")), (Bytes.toBytes("OG" + sep + "DC"))))
    //get dataframe
    val df = HBaseSupport.toHbaseBulkGetDataFrame(tableName, rdd_key, 2)
    df.count() shouldBe 2

  }

  "A dataframe" should "read/write to a table with one column family" ignore {
    val keys_index = Array(1, 2)
    val sep = "##"
    HBaseSupport.toHbaseBulkPutDataframe(tableName, testDF, keys_index, sep, cf)
    val rdd_key1 = sc.parallelize(Array((Bytes.toBytes("value2##value3"))))
    val df1 = HBaseSupport.toHbaseBulkGetDataFrame(tableName, rdd_key1, 2)
    df1.show()
    df1.count() shouldBe 1
  }

  "A distributedScan using table name and column family " should " return a dataframe " in {
    val keys_index = Array(3, 6)
    val sep = "##"
    //convert to hbase records
    val rdd = HBaseSupport.toDFToHbaseRDD(header, rows, keys_index, sep)(cf)
    //bulkput
    HBaseSupport.toHbaseBulkPutRDD(tableName, rdd)
    val df = HBaseSupport.toHbaseBulkScanDataFrame(tableName, 2, cf, 2)
    df.count() shouldBe 2
  }
}