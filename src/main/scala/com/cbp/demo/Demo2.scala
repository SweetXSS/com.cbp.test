package com.cbp.demo

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

object Demo2 {
  def main(args: Array[String]): Unit = {
    val readTable = args(0)
    val writeTable = args(1)
    val ss = SparkSession.builder().getOrCreate()
    //设置hbase配置信息
    val hbaseConf = HBaseConfiguration.create()
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, readTable)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,writeTable)
    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE,"100")
    //    hbaseConf.set("hbase.mapreduce.hfileoutputformat.table.name", writeTable)
    hbaseConf.set("hbase.zookeeper.quorum", "masterhost1, masterhost2, masterhost3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //读取hbase数据
    val hbaserdd = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hbaserdd.foreachPartition(println)
    print("*********************************************************")
    hbaserdd.map(t =>{t._2.getFamilyMap(Bytes.toBytes("fpxx"))}).foreachPartition(println)
    ss.close()
  }
}
