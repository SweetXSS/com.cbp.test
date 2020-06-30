package com.cbp.spark_hive_hbase

import com.cbp.util.BaseUtil
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel


object SparkReadHiveToHbasePut {
  //设置日志级别
  Logger.getLogger("org").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    //外部传参，hive表名、hbase表名、年份的后两位
    val hiveTable = args(0)
    val hbaseTable = args(1)
    val month = args(2)
    //创建SparkSession
    val ss = SparkSession.builder().appName("hiveTohbase")
      .enableHiveSupport()
      .getOrCreate()

    //读取hive数据
    val hiveDF: DataFrame = ss.sql(
      s"""
         |select xfmc,xfsbh,xfdzdh,xfyhzh,gfmc,gfsbh,gfdzdh,gfyhzh,fpdm,fphm,fp_lb,je,se,jshj,kpr,kprq,kpyf,kpjh,qdbz,zfbz,zfsj
         |from ${hiveTable}
         |""".stripMargin)

    //指定列族、列
    val columnf = "fpxx"
    val columnNames = Array("xfmc", "xfsbh", "xfdzdh", "xfyhzh", "gfmc", "gfsbh", "gfdzdh", "gfyhzh", "fpdm", "fphm", "fp_lb", "je", "se", "jshj", "kpr", "kprq", "kpyf", "kpjh", "qdbz", "zfbz", "zfsj", "source", "gfqybm", "xfqybm")
    ss.sparkContext.broadcast(columnNames)
    //设置hbase配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
    hbaseConf.set("hbase.zookeeper.quorum", "masterhost1, masterhost2, masterhost3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //配置job信息
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputValueClass(classOf[Put])

    //创建输出rdd，put方式输出到hbase
    mkOutputRdd(hiveDF, columnf, columnNames, month)
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
    ss.close()
  }

  //获取到hive数据df，转换为rdd
  def mkOutputRdd(hiveDF: DataFrame, columnf: String, columnNames: Array[String], month: String) = {
    val rdd1 = hiveDF.rdd.filter(row => row.length > 0 && !row(0).toString.contains("?") && !nullDecide(row(0)) && !nullDecide(row(1)))
      .coalesce(400, true)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val rdd2 = rdd1.map(row => {
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
      .filter(row => !"-1".equals(row(23)))
      .filter(row => !nullDecide(row(8)))
      .filter(row => !nullDecide(row(9)))
      .map(row => {
        var yy = row(16).toString.trim.substring(2, 4)
        if (nullDecide(row(16))) {
          yy = month
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
        (new ImmutableBytesWritable, put)
      })
    rdd1.unpersist()
    rdd2
  }

  //rowkey设计qybm+yy+fpbm
  def mkRowKey(xfqybm: String, fpdm: String, fphm: String, yy: String): String = {
    val fpbm = BaseUtil.getFpbm(fpdm, fphm)
    val rowKey = xfqybm + yy + fpbm
    rowKey
  }

  //获取qybm企业编码
  def getQybm(xfmc: String, xfsbh: String, gfmc: String) = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName1 = "nbdpt:qy_mc_bm"
    val table = conn.getTable(TableName.valueOf(tableName1))
    val tableName2 = "nbdpt:qy_xydm_bm"

    val getX = new Get(Bytes.toBytes(xfmc))
    val result = table.get(getX)

    var xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    if (nullDecide(xfqybm)) {
      val table = conn.getTable(TableName.valueOf(tableName2))
      val get = new Get(Bytes.toBytes(xfsbh))
      val result = table.get(get)
      xfqybm = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
    }
    if (nullDecide(xfqybm)) {
      xfqybm = "-1"
    }
    var gfqybm = "-1"
    if (!nullDecide(gfmc)) {
      val get = new Get(Bytes.toBytes(gfmc))
      val result = table.get(get)
      val r1 = Bytes.toString(result.getValue(Bytes.toBytes("qyxx"), Bytes.toBytes("qybm")))
      if (!nullDecide(r1)) {
        gfqybm = r1
      }
    }
    conn.close()
    (xfqybm, gfqybm)
  }

  //处理空字段
  def nullHandle(str: String): String = {
    if (str == null || "".equals(str)) {
      "null"
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

