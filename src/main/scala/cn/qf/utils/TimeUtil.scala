package cn.qf.utils

import java.text.SimpleDateFormat

object TimeUtil {
  //时间工具类
  def costtiem(startTime:String,stopTime:String):Long={
    val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    // 开始时间
    val st: Long = df.parse(startTime.substring(0,17)).getTime
    // 结束时间
    val et = df.parse(stopTime).getTime
    et-st
  }
}
