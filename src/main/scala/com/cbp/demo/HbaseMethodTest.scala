package com.cbp.demo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object HbaseMethodTest {
  def main(args: Array[String]): Unit = {
    //创建put对象
    val put = new Put(Bytes.toBytes(""))
    //添加数据
    put.addColumn(Bytes.toBytes(""),Bytes.toBytes(""),Bytes.toBytes(""))
    //获取值
    put.get(Bytes.toBytes(""),Bytes.toBytes(""))
    //获取时间戳
    put.getTimestamp
    //获取列族
    put.getFamilyCellMap
    //查询特定单元格，不会全表扫描
    put.has(Bytes.toBytes(""),Bytes.toBytes(""))
    put.getRow
    val conf = HBaseConfiguration.create()
    val table = new HTable(conf,"")
  }
}
