package com.cbp.spark_hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SparkHbaseFilter {
  def main(args: Array[String]): Unit = {
    val tableName = "nbpttest:student"
    val columnFamily = "fm1"
    val columnNames = Array("name")
    //创建spark sql入口类SparkSession
    val ss = SparkSession.builder().getOrCreate()
    //读取hbase数据
    val stuRDD = readHbaseRDD(ss, tableName)
    //使用隐式转换rdd转DataFrame
//    import ss.implicits._
//    val f: DataFrame = stuRDD.toDF

//    stuRDD.map(rdd => {
//      val k_str = Bytes.toString(rdd._2.getRow)
//      val v_str = Bytes.toString(rdd._2.getValue(Bytes.toBytes("fm1"), Bytes.toBytes("name")))
//      (k_str, v_str)
//    }).saveAsTextFile("./stu")

    //使用自编方法将rdd转换为DataFrame
    val stu_df: DataFrame = hbaseRddToDF(ss, stuRDD,  columnFamily,columnNames)
      //注册为临时表
      stu_df.createOrReplaceTempView("fp_xx_mx_1")
      stu_df.sqlContext.sql(
        s"""
           |select * from fp_xx_mx_1 where fpdm = fphm and flbm = ""
           |""".stripMargin).write.mode("append").csv("./stu")
    stu_df.show(10)
    ss.close()
  }

  //读取hbase数据生成rdd
  def readHbaseRDD(ss:SparkSession,tableName:String)={
    val hbaseConf = HBaseConfiguration.create()
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    //Spark读取HBase，需要使用SparkContext提供的newAPIHadoopRDD API将表的内容以RDD的形式加载到Spark中。
    val stuRDD = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    stuRDD
  }


  //RDD转dataDrame方法，三个参数（SparkSession，columnFamily列族，columnNames；列：一个集合）
  def hbaseRddToDF(ss:SparkSession,hbaseRdd: RDD[(ImmutableBytesWritable, Result)], columnFamily:String,columnNames: Array[String]): DataFrame = {
    //通过可变array来封装Array[StructField]属性数组
    val structFields = ArrayBuffer(StructField("row_key", StringType))
    columnNames.foreach(y => {
      structFields.append(StructField(y, StringType))
    })
    //定义schema，StructType是一个case class，可以有多个StructField，源码case class StructType(fields: Array[StructField])
    val dfschema = StructType(structFields.toArray)

    //封装rowRDD
    val rowRdd = hbaseRdd.map(rdd => {
      val values = ArrayBuffer[String](Bytes.toString(rdd._2.getRow))
      columnNames.foreach(columns => {
          values.append(Bytes.toString(rdd._2.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columns))))
      })
      Row.fromSeq(values.toSeq)
    })
    //通过createDataFrame方法将rdd转换为dataFrame，两个参数(rowRDD,StructType)
    val rddToDF = ss.createDataFrame(rowRdd, dfschema)
    rddToDF
  }
}
