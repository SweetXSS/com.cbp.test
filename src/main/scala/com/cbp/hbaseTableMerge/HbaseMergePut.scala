package com.cbp.hbaseTableMerge

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
/**
 * 使用put方式
 * 弊端：数据量大，没有预分区，写入过程会造成 region拒绝写入（原因请百度），小批量数据ok
 * */
object HbaseMergePut {
  //设置日志级别
  Logger.getLogger("org").setLevel(Level.INFO)
  def main(args: Array[String]): Unit = {
    //外部传参，读表、写表、开始row、结束row、临时文件路径、重分区数、列族
    val readTable = args(0)
    val writeTable = args(1)
    val startRow = args(2)
    val stopRow = args(3)
    val partition = args(4)
    val columnf = args(5)
    //创建spark sql入口类SparkSession
    val ss = SparkSession.builder().getOrCreate()
    //设置hbase配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, readTable)
    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRow)
    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, stopRow)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, writeTable)
    //配置job信息
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputValueClass(classOf[Put])
    //读取hbase数据,处理，写入
    //阶段1
    val rdd = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
      .repartition(Integer.valueOf(partition))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //阶段2
      rdd.map(t => {
        val put = new Put(t._2.getRow)
        val cols = ArrayBuffer[String]()
        val colsMap = t._2.getFamilyMap(Bytes.toBytes(columnf))
        import scala.collection.JavaConversions._
        for (entry <- colsMap.entrySet()) {
          cols.append(Bytes.toString(entry.getKey))
        }
        cols.foreach(col => {
          put.addColumn(Bytes.toBytes(columnf), Bytes.toBytes(col), t._2.getValue(Bytes.toBytes(columnf), Bytes.toBytes(col)))
        })
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
