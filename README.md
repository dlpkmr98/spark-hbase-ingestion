spark-hbase-ingestion
spark-hbase-ingestion using dataframe


/**
   * method for convert records for inserting into HBase
   *
   * @param records
   * @param cf Column Family
   * @return
   */
   
  def toHbaseRecords(records: Array[(String, Array[(String, String)])], cf: String): RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    sc.parallelize(records.map(x => (ct(x._1), x._2.map(x => (ct(cf), ct(x._1), ct(x._2))))))
  }

  /**
   * method for inserting bulk rdd in HBase table
   * @param tableName
   * @param rdd
   */
   
  def toHbaseBulkPutRDD(tableName: String, rdd: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])])(implicit conf: Configuration): Unit = {
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

  }

  /**
   * method for inserting bulk dataframe in HBase table . Dataframe rows should be string
   * @param tableName
   * @param df
   * @param rowkeysIndex
   * @param rowKeyDelimiter
   * @param cf
   */
   
  def toHbaseBulkPutDataframe(tableName: String, df: DataFrame, rowkeysIndex: Array[Int],
                              rowKeyDelimiter: String, cf: String)(implicit conf: Configuration) = {
    try {
      val h = df.columns.toSeq
      val v = df.map(x => {
        x.toSeq.toArray.map(_.toString())
      }).toArray()
      val hbaseRDD = toDFToHbaseRDD(h, v, rowkeysIndex, rowKeyDelimiter)(cf)
      toHbaseBulkPutRDD(tableName, hbaseRDD)
    } catch {
      case ex: Exception => throw ex
    }
  }

  /**
   * method for get rdd using row keys
   *
   * @param tableName
   * @param rdd_key   rdd of row_keys
   * @param batchSize To limit the number of columns if your table has very wide
   * rows (rows with a large number of columns), use setBatch(int batch) and set it to
   * the number of columns you want to return in one batch. A large number of columns is not a recommended design pattern
   * @return
   */
   
  def toHbaseBulkGetRDD(tableName: String, rdd_key: RDD[Array[Byte]], batchSize: Int)(implicit conf: Configuration): RDD[scala.collection.mutable.Map[String, String]] = {
    hbaseContext.bulkGet[Array[Byte], scala.collection.mutable.Map[String, String]](
      TableName.valueOf(tableName),
      batchSize,
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
  }

  /**
   * method for get dataframe using keys
   *
   * @param tableName
   * @param rdd_key   rdd of row_keys
   * @param batchSize To limit the number of columns if your table has very wide
   * rows (rows with a large number of columns), use setBatch(int batch) and set it to
   * the number of columns you want to return in one batch. A large number of columns is not a recommended design pattern
   * @return
   */
   
  def toHbaseBulkGetDataFrame(tableName: String, rdd_key: RDD[Array[Byte]], batchSize: Int)(implicit conf: Configuration): DataFrame = {
    val rdd = toHbaseBulkGetRDD(tableName, rdd_key, batchSize)
    try {
      val zipRdd = rdd.filter(!_.isEmpty).map(x => x.toList.sortBy(_._1).unzip)
      val schema = StructType(zipRdd.first()._1.map(k => StructField(k, StringType, nullable = false)))
      val rows = zipRdd.map(_._2).map(x => (Row(x: _*)))
      sqlCtx.createDataFrame(rows, schema)
    } catch {
      case ex: UnsupportedOperationException => throw ex
    }
  }

  /**
   * @param tableName
   * @param maxResultSize To specify a maximum result size, use setMaxResultSize(long), with the number of bytes.
   * The goal is to reduce IO and network.
   * @param cf column family
   * @param batchSize To limit the number of columns if your table has very wide
   * rows (rows with a large number of columns), use setBatch(int batch) and set it to
   * the number of columns you want to return in one batch. A large number of columns is not a recommended design pattern
   * @return
   */
   
  def toHbaseBulkScanDataFrame(tableName: String, maxResultSize: Long, cf: Array[Byte], batchSize: Int)(implicit conf: Configuration): DataFrame = {
    println("HBASE: toHbaseBulkScanDataFrame...........")
    val scan = new Scan()
    scan.setMaxResultSize(maxResultSize)
    scan.addFamily(cf)
    println("Scanning hbase............. ")
    val rdd_key = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan).map(_._1.get())
    println("rdd_key:...... " + rdd_key.map(x => Bytes.toString(x)).collect().foreach(println))
    toHbaseBulkGetDataFrame(tableName, rdd_key, batchSize)
  }

   /**
 * @param headers
 * @param records
 * @param keys_index
 * @param rowKeyDelimiter
 * @return String => RDD
 */
def toDFToHbaseRDD(headers: Seq[String], records: Array[Array[String]],keys_index: Array[Int], rowKeyDelimiter: String) = (cf: String) => {
    sc.parallelize(records.flatMap(x => {
      val keys = keys_index.map(f => x(f)).mkString(rowKeyDelimiter)
      Array((keys, Array(headers zip x: _*)))
    }).map(x => (ct(x._1), x._2.map(x => (ct(cf), ct(x._1), ct(x._2))))))

  }

