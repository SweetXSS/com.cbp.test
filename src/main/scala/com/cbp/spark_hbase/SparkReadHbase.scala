package com.cbp.spark_hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    //设置表名
    val tableName1 = "nbdpt:qy_mc_bm"
    val tableName2 = "nbdpt:qy_xydm_bm"
    val qymc = args(0)
    val xfsbh = args(1)
    //创建hbase连接
    val conf = HBaseConfiguration.create
    val conn = ConnectionFactory.createConnection(conf)
    //获取table对象
    val table1 = conn.getTable(TableName.valueOf(tableName1))
    val table2 = conn.getTable(TableName.valueOf(tableName1))
    //根据rowkey获取到get对象
    val get1 = new Get(Bytes.toBytes(qymc))
    //获取到结果集
    val result1 = table1.get(get1)
    //取出value值
    val qybm =Bytes.toString(result1.getValue(Bytes.toBytes("qyxx"),Bytes.toBytes("qybm")))
    val get2 = new Get(Bytes.toBytes(xfsbh))
    val result2 = table2.get(get2)
    val qybm1 =Bytes.toString(result2.getValue(Bytes.toBytes("qyxx"),Bytes.toBytes("qybm")))
    System.out.println("*****************************"+qybm + qybm1+"*************************************************")
   //结果*******nullnull*******
    conn.close()
  }
}
