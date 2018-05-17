package com.org.dilip.spark.ingestion.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.security.UserGroupInformation
import com.org.dilip.spark.ingestion.config.ConfigLoader

/**
 * Wrapper for HBaseConfiguration
 * With hbase-site.xml
 *
 * If you happen to have on the classpath hbase-site.xml with the right configuration parameters, you can just do
 * * implicit val config = HBaseConfig()
 * Otherwise, you will have to configure HBase RDD programmatically.
 *
 * With a case class
 *
 *
 *
 * With a Hadoop configuration object
 *
 * Finally, HBaseConfig can be instantiated from an existing org.apache.hadoop.conf.Configuration
 *
 * val conf: Configuration = ...
 * implicit val config = HBaseConfig(conf)
 */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get: Configuration = HBaseConfiguration.create(defaults)
}

/**
 * @author dlpkmr98
 *
 */
object HBaseConfig {
  /**
   * @param conf
   * @return
   */
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  /**
   * @return
   */
  def apply: HBaseConfig = {
    val conf = HBaseConfiguration.create(new Configuration(false))
    conf.addResource(new Path(ConfigLoader.hadoopCoreXmlPath))
    conf.addResource(new Path(ConfigLoader.hbaseXmlPath))
    apply(conf)
  }

}