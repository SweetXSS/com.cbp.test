package com.cbp.demo

import java.util

import com.cbp.spark_hive_hbase.SparkHbase2019.nullHandle
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

object Demo1 {
  def main(args: Array[String]): Unit = {
    val str = args(0)
    print("**********"+getQybm(str)+"****************")
    val ss = new SparkContext()

//    //组装scan语句
//    val scan=new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
//    scan.setCacheBlocks(false)
//    scan.addFamily(Bytes.toBytes("ks"));
//    scan.addColumn(Bytes.toBytes("ks"), Bytes.toBytes("data"))
//    //将scan类转化成string类型
//    val proto= ProtobufUtil.toScan(scan)
//    val ScanToString = Base64.encodeBytes(proto.toByteArray());
//    conf.set(TableInputFormat.SCAN,ScanToString)

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
}
