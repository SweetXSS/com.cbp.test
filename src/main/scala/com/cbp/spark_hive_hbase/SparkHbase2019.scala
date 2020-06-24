package com.cbp.spark_hive_hbase

import com.cbp.util.BaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

object SparkHbase2019 {

  //设置日志显示级别
  Logger.getLogger("org").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val hiveTable = args(0)
    val hbaseTable = args(1)
    //创建SparkSession
    val ss = SparkSession.builder().appName("removeData")
      .enableHiveSupport()
      .getOrCreate()

    //读取hive数据
    val hiveDF: DataFrame = ss.sql(
      s"""
         |select
         |xf_nsrsbh,gf_nsrsbh,fpdm,fphm,fpmxxh,flbm,kprq,kpyf,spmc,jldw,je,se,slv,zfbz,sjly,ext
         |from ${hiveTable}
         |""".stripMargin)
    //指定列族、列
    val columnf = "fpmx"
    val columnNames = Array("xf_nsrsbh", "gf_nsrsbh", "fpdm", "fphm", "fpmxxh", "flbm", "kprq", "kpyf", "spmc", "jldw", "je", "se", "slv", "zfbz", "sjly", "ext")
//    ss.sparkContext.broadcast(columnNames)

    val newRdd = hiveDF.rdd
      .filter(row => row.length > 0 && !nullDecide(row(0)))
      .filter(row => !nullDecide(row(2)))
      .filter(row => !nullDecide(row(3)))
      .filter(row => !nullDecide(row(4)))
      .filter(row => !nullDecide(row(7)) && row(7).toString.length > 4)
      .map(row => {
        val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
        buffer.update(14, "hive")
        val schema: StructType = row.schema
        val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
        newRow
      })
      .map(row => {
        val xfqybm = getQybm(String.valueOf(row(0)).trim)
        val fpdm = String.valueOf(row(2)).trim
        val fphm = String.valueOf(row(3)).trim
        val id = String.valueOf(row(4)).trim
        val yy = String.valueOf(row(7)).trim.substring(2, 4)
        val rowKey = mkRowKey(xfqybm, fpdm, fphm, yy, id)
        val put = new Put(Bytes.toBytes(rowKey))
        columnNames.foreach(col => {
          put.addColumn(Bytes.toBytes(columnf), Bytes.toBytes(col), Bytes.toBytes(nullHandle(row.getAs(col))))
        })
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
        (new ImmutableBytesWritable, put)
      }).cache()
        newRdd.map(tuple=> Bytes.toString(tuple._2.getRow))
          .take(100).foreach(println)
    println("*******************************************************************************************")
        newRdd.filter(tuple=> !Bytes.toString(tuple._2.getRow).contains("-"))
          .take(1000).foreach(println)
  }

  def getQybm(xf_nsrsbh: String) = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName = "nbdpt:qy_xydm_bm"
    val table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(xf_nsrsbh))
    val result = table.get(get)
    val xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    conn.close()
    nullHandle(xfqybm)
  }

  //rowkey设计qybm+yy+fpbm
  def mkRowKey(xfqybm: String, fpdm: String, fphm: String, yy: String, id: String): String = {
    val fpbm = BaseUtil.getFpbm(fpdm, fphm)
    val rowKey = xfqybm + yy + fpbm + id
    rowKey
  }

  //处理空字段
  def nullHandle(str: String): String = {
    if (str == null || str == "" || str == "null") {
      "-1"
    } else {
      str
    }
  }

  //判断空字段
  def nullDecide(str: Any): Boolean = {
    if (str == null || str == "" || str == "null") {
      true
    } else {
      false
    }
  }
}
