package com.cbp.spark_hive

import com.cbp.util.BaseUtil
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 可能遇到的问题：
 * 1、源数据库连接失败   解决：将hive-site.xml 复制到resources文件中，或者在conf中配置元数据信息
 * 2、sql语句中表在某个数据库中，必须写上数据库名，database.tableName 否则找不到表报错
 **/
object SparkReadHive {
  Logger.getLogger("org").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //创建sparksession对象，开启hive支持enableHiveSupport()
    val ss = SparkSession.builder().appName("spark_hive_hbase")
      .enableHiveSupport()
      .getOrCreate()
    //使用sql语句获取hive表数据，类型为DataFrame
    val hiveDF: DataFrame = ss.sql(
      s"""
         |select xfmc,xfsbh,xfdzdh,xfyhzh,gfmc,gfsbh,gfdzdh,gfyhzh,fpdm,fphm,fp_lb,je,se,jshj,kpr,kprq,kpyf,kpjh,qdbz,zfbz,zfsj
         |from nbdltest.xxfp2016_demo
         |""".stripMargin)
    //    val partitions: Int = hiveDF.rdd.getNumPartitions
    //    val lines: Long = hiveDF.rdd.count()
    //    println("***********************"+lines+"****"+partitions+"*********************************")

    //指定列族、列
    val columnf = "fpxx"
    val columnNames = Array("xfmc", "xfsbh", "xfdzdh", "xfyhzh", "gfmc", "gfsbh", "gfdzdh", "gfyhzh", "fpdm", "fphm", "fp_lb", "je", "se", "jshj", "kpr", "kprq", "kpyf", "kpjh", "qdbz", "zfbz", "zfsj", "source", "gfqybm", "xfqybm")


    val value: RDD[(ImmutableBytesWritable, Put)] = hiveDF.rdd.filter(row => row.length > 0 && !row(0).toString.contains("?") && row(0) != "no" && row(0) != "" && row(0) != null && row(0) != "null" && row(1) != "no" && row(1) != "" && row(1) != null && row(1) != "null")
      .map(row => {
        val xfmc = row(0).toString.trim
        val xfsbh = row(1).toString.trim
        val gfmc = row(4).toString.trim
        val source = "hive"
        val xfqybm = getQybm(xfmc, xfsbh, gfmc)._1
        val gfqybm = getQybm(xfmc, xfsbh, gfmc)._2
        val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
        buffer.append(source)
        buffer.append(gfqybm)
        buffer.append(xfqybm)
        val schema: StructType = row.schema
          .add("source", StringType)
          .add("gfqybm", StringType)
          .add("xfqybm", StringType)
        val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
        newRow
      })
      .filter(row => row(23) != null && row(23) != "null" && row(8) != "" && row(8) != null && row(8) != "null" && row(9) != "" && row(9) != null && row(9) != "null")
      .map(row => {
        var yy = row(16).toString.trim.substring(2, 4)
        if (row(16) == null || row(16) == "" || row(16) == "null") {
          yy = "16"
        }
        val fpdm = row(8).toString.trim
        val fphm = row(9).toString.trim
        val xfqybm = row(23).toString.trim
        val rowKey = mkRowKey(xfqybm, fpdm, fphm, yy)

        val put = new Put(Bytes.toBytes(rowKey))
        columnNames.foreach(col => {
          put.addColumn(Bytes.toBytes(columnf), Bytes.toBytes(col), Bytes.toBytes(nullHandle(row.getAs(col))))
        })
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
        put.size()
        (new ImmutableBytesWritable, put)
      })
    value.take(1000).foreach(println)
      ss.close()
  }


  //rowkey设计qybm+yy+fpbm
  def mkRowKey(xfqybm: String, fpdm: String, fphm: String, yy: String): String = {
    val fpbm = BaseUtil.getFpbm(fpdm, fphm)
    val rowKey = xfqybm + yy + fpbm
    rowKey
  }

  def getQybm(xfmc: String, xfsbh: String,gfmc: String) = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName1 = "nbdpt:qy_mc_bm"
    val table = conn.getTable(TableName.valueOf(tableName1))
    val tableName2 = "nbdpt:qy_xydm_bm"

    val getX = new Get(Bytes.toBytes(xfmc))
    val result = table.get(getX)

    var xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    if (xfqybm == null || xfqybm == "null") {
      val table = conn.getTable(TableName.valueOf(tableName2))
      val get = new Get(Bytes.toBytes(xfsbh))
      val result = table.get(get)
      xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    }
    if(xfqybm == null || xfqybm == "null" ){
      xfqybm = "-1"
    }
    var gfqybm = ""
    if (gfmc == null || gfmc == "" || gfmc == "null" ) {
      gfqybm = "-1"
    } else {
      val get = new Get(Bytes.toBytes(gfmc))
      val result = table.get(get)
      gfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    }
    if(gfqybm == null || gfqybm == "" || gfqybm == "null"){
      gfqybm = "-1"
    }
    (xfqybm,gfqybm)
  }

  //处理空字段
  def nullHandle(str: String): String = {
    if (str == null || "".equals(str)) {
      "null"
    } else {
      str
    }
  }
}
