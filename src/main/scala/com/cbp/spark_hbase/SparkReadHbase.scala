package com.cbp.spark_hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    val readTable = args(0)
    val ss = SparkSession.builder().getOrCreate()
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, readTable)
    val rdd = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    rdd.map(t=>{
      val cols = ArrayBuffer[String]()
      val colsMap = t._2.getFamilyMap(Bytes.toBytes("fpxx"))
      import scala.collection.JavaConversions._
      for (entry <- colsMap.entrySet()) {
        cols.append(Bytes.toString(entry.getKey))
      }
      cols
    }).take(100).foreach(println)
  }

  def getConn(tableName:String)={
    //设置hbase配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN_TIMESTAMP, tableName)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_START, tableName)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_END, tableName)
    //    val scan = new Scan()
    //    scan.setCaching(100000)
    //    scan.setCacheBlocks(flase)
    //    scan.setBatch(1000000)
    //
    //    val proto = ProtobufUtil.toScan(scan)
    //    val ss = Base64.encodeBytes(proto.toByteArray)
    //    hbaseConf.set(TableInputFormat.SCAN,ss)
    //获取hbase连接
    val conn = ConnectionFactory.createConnection(hbaseConf)
    //获取table对象
    val table = conn.getTable(TableName.valueOf(tableName))
    //根据rowkey获取到get对象
    val get = new Get(Bytes.toBytes(""))
    //获取到结果集
    val result1 = table.get(get)
    //取出value值
    val qybm =Bytes.toString(result1.getValue(Bytes.toBytes("qyxx"),Bytes.toBytes("qybm")))
    conn.close()
    qybm
  }

}
