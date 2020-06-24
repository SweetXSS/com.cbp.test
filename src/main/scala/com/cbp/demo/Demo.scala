package com.cbp.demo

import com.cbp.util.BaseUtil
import org.apache.hadoop.hbase.util.Bytes

object Demo {
  def main(args: Array[String]): Unit = {
    val str = mkRowKey("deded", "3600143130", "04538355", "16", "10")
    println("".length)
    println("UYF0BAK618G68T8J0665338".contains("-"))
    println(String.valueOf("201607").substring(2,4))
  }

  //rowkey设计qybm+yy+fpbm
  def mkRowKey(xfqybm: String, fpdm: String, fphm: String, yy: String, id: String): String = {
    val fpbm = BaseUtil.getFpbm(fpdm, fphm)
    val rowKey = xfqybm + yy + fpbm + id
    rowKey
  }
}
