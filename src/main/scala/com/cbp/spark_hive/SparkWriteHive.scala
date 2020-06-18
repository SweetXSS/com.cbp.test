package com.cbp.spark_hive

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 可能遇到的问题：
 * 1、源数据库连接失败   解决：将hive-site.xml 复制到resources文件中，或者在conf中配置元数据信息
 * 2、sql语句中表在某个数据库中，必须写上数据库名，database.tableName 否则找不到表报错
 **/
object SparkWriteHive {
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
    val result = hiveDF.rdd.filter(row => row.length > 0 && !row(0).toString.contains("?") && row(0) != "no" && row(0) != "" && row(0) != null && row(0) != "null" && row(1) != "no" && row(1) != "" && row(1) != null && row(1) != "null")
      .coalesce(hiveDF.rdd.getNumPartitions % 2 + 100,true)
      .map(row => {
        val xfmc = row(0).toString.trim
        val xfsbh = row(1).toString.trim
        val gfmc = row(4).toString.trim
        val source = "hive"
        val xfqybm = getXfqybm(xfmc, xfsbh)
        val gfqybm = getGfqybm(gfmc)
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
    result.take(10000).foreach(println)

    ss.close()
  }

  def getXfqybm(xfmc: String, xfsbh: String): String = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName1 = "nbdpt:qy_mc_bm"
    val tableName2 = "nbdpt:qy_xydm_bm"
    val table = conn.getTable(TableName.valueOf(tableName1))
    val get = new Get(Bytes.toBytes(xfmc))
    val result = table.get(get)
    var xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    if (xfqybm == null || xfqybm == "null") {
      val table = conn.getTable(TableName.valueOf(tableName2))
      val get = new Get(Bytes.toBytes(xfsbh))
      val result = table.get(get)
      xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    }
    xfqybm
  }

  def getGfqybm(gfmc: String): String = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName = "nbdpt:qy_mc_bm"
    val table = conn.getTable(TableName.valueOf(tableName))
    if (gfmc == null || gfmc == "" || gfmc == "null") {
      ""
    } else {
      val get = new Get(Bytes.toBytes(gfmc))
      val result = table.get(get)
      val gfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
      gfqybm
    }

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
