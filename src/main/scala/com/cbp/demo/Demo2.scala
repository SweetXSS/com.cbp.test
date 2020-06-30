package com.cbp.demo

import com.cbp.spark_hive_hbase.SparkHbase2019.nullDecide
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val hiveTable = args(0)
    val writeTable = args(1)
    val ss = SparkSession.builder().getOrCreate()
    //读取hive数据
    val hiveDF: DataFrame = ss.sql(
      s"""
         |select
         |xf_nsrsbh,gf_nsrsbh,fpdm,fphm,fpmxxh,flbm,kprq,kpyf,spmc,jldw,je,se,slv,zfbz,sjly,ext
         |from ${hiveTable}
         |""".stripMargin)
    println(hiveDF.rdd.getNumPartitions)
    val partitions = hiveDF.rdd
      .filter(row => row.length > 0 && !nullDecide(row(0)))
      .filter(row => !nullDecide(row(2)))
      .filter(row => !nullDecide(row(3)))
      .filter(row => !nullDecide(row(4)))
      .filter(row => !nullDecide(row(7)) && row(7).toString.length > 4)
        .getNumPartitions
    println(partitions)
    ss.close()
  }
}
