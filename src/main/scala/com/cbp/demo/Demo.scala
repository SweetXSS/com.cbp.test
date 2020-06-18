package com.cbp.demo

import com.cbp.util.BaseUtil

object Demo {
  def main(args: Array[String]): Unit = {
    val str = mkRowKey("deded", "3600143130", "04538355", "16", "10")
    print(str.contains("-"))
  }

  //rowkey设计qybm+yy+fpbm
  def mkRowKey(xfqybm: String, fpdm: String, fphm: String, yy: String, id: String): String = {
    val fpbm = BaseUtil.getFpbm(fpdm, fphm)
    val rowKey = xfqybm + yy + fpbm + id
    rowKey
  }
}
