package com.cbp.hbaseTableMerge

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

object HbaseMergeBulk2 {
  //设置日志级别
  Logger.getLogger("org").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //外部传参，读表、写表、开始row、结束row、临时文件路径、重分区数、列族
    val readTable = args(0)
    val writeTable = args(1)
    val startTime = args(2)
    val stopTime = args(3)
    val filePath = args(4)
    val partition = args(5)
    val columnf = args(6)
    //创建spark sql入口类SparkSession
    val ss = SparkSession.builder().getOrCreate()
    //设置hbase配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, readTable)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_START, startTime)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_END, stopTime)
    hbaseConf.set("hbase.mapreduce.hfileoutputformat.table.name", writeTable)
    hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 5000)
    //获取hbase连接，获取表名、region信息
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val regionLocator = conn.getRegionLocator(TableName.valueOf(writeTable))
    val table = conn.getTable(TableName.valueOf(writeTable))
    //配置job信息
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor)
    //读取hbase数据并处理
    //阶段1
    val rdd = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
      .repartition(Integer.valueOf(partition))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //阶段2
    val rdd1 = rdd.flatMap(t => {
      val values = ArrayBuffer[(String, (String, String, String))]()
      val cols = ArrayBuffer[String]()
      val colsMap = t._2.getFamilyMap(Bytes.toBytes(columnf))
      import scala.collection.JavaConversions._
      for (entry <- colsMap.entrySet()) {
        cols.append(Bytes.toString(entry.getKey))
      }
      cols.foreach(col => {
        values.append((Bytes.toString(t._2.getRow), (columnf, col, Bytes.toString(t._2.getValue(Bytes.toBytes(columnf), Bytes.toBytes(col))))))
      })
      values
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd.unpersist()
    //阶段3
    rdd1.sortBy(x => (x._1, x._2._1, x._2._2))
      .map(rdd => {
        val rowKey = Bytes.toBytes(rdd._1)
        val family = Bytes.toBytes(rdd._2._1)
        val colum = Bytes.toBytes(rdd._2._2)
        val value = Bytes.toBytes(rdd._2._3)
        (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, value))
      }).saveAsNewAPIHadoopFile(filePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf)
    //创建bulk load 对象并加载
    val load = new LoadIncrementalHFiles(hbaseConf)
    load.doBulkLoad(new Path(filePath), conn.getAdmin, table, regionLocator)

    table.close()
    conn.close()
    ss.close()
  }
}
