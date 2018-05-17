# spark-hbase-mock
Hbase mock testing using spark scala test


Running Hbase Testing Utility On Windows

The HBase Testing Utility is a vital tool for anyone writing HBase applications. 
It sets up (and tears down) a lightweight HBase instance locally to allow local integration tests. 
I’ve previously discussed this and how to use it with BDD Cucumber tests in this blog post, complete with working repo. 
However, it is not trivially easy to get working on Windows machines, nor is it documented anywhere.

In this blog post, I’ll show how to get the HBase Testing Utility running on Windows machines, no admin access required. 
There’s no accompanying GitHub project for this post, as it’s fairly short and generic. 
I’ll assume you already have a working HBase Testing Utility test that runs on Unix, and you want to port it to Windows.

Download the https://github.com/sardetushar/hadooponwindows

Download/clone Winutils. This contains several Hadoop versions compiled for Windows.
Go to Control Panel, and find Edit environment variables for your account in System.
Add the following user variables:
hadoop.home.dir=<PATH_TO_DESIRED_HADOOP_VERSION> (in my case, this was C:\Users\bwatson\apps\hadoop-2.8.3)
HADOOP_HOME=<PATH_TO_DESIRED_HADOOP_VERSION> (as above)
append %HADOOP_HOME%/bin to Path

Before calling new HBaseTestingUtility(); 
the temporary HBase data directory needs to be set. 
Add System.setProperty("test.build.data.basedirectory", "C:/Temp/hbase"); to the code. 
This path can be changed, but it’s important to keep it short. Using the JUnit TemporaryFolder or the default path results in paths 
too long, and shows an error similar to
java.io.IOException: Failed to move meta file for ReplicaBeingWritten, blk_1073741825_1001, RBW.



Put RDD Ex:


val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })
      
      
  Get RDD Ex: 
  
  
  val rdd_key = sc.parallelize(Array((Bytes.toBytes("key1")), (Bytes.toBytes("key2"))))

    val getRdd = hbaseContext.bulkGet[Array[Byte], scala.collection.mutable.Map[String, String]](
      TableName.valueOf(tableName),
      2,
      rdd_key,
      record => {
        new Get(record)
      },
      (result: Result) => {
        val map = scala.collection.mutable.Map[String, String]()
        if (result.listCells() != null) {
          map += ("row_key" -> Bytes.toString(result.getRow))
          val it = result.listCells().iterator()
          while (it.hasNext) {
            val cell = it.next()
            val q = Bytes.toString(CellUtil.cloneQualifier(cell))
            map += (q -> Bytes.toString(CellUtil.cloneValue(cell)))
          }
        }
        map
      })
      
    Converting RDD to Spark DataFrame:
    
    val zipRdd = getRdd.filter(!_.isEmpty).map(x => x.toList.sortBy(_._1).unzip)
    val schema = StructType(zipRdd.first()._1.map(k => StructField(k, StringType, nullable = false)))
    val rows = zipRdd.map(_._2).map(x => (Row(x: _*)))

    val priceDF = sqlCtx.createDataFrame(rows, schema)
    priceDF.show()
