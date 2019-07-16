package cn.qf.utils

import java.util

import org.apache.kafka.common.TopicPartition

object MyJedisOffset {
  def apply(groupId: String):Map[TopicPartition,Long] = {
    //创建Map的topic，partition，offset
    var formdbOffset = Map[TopicPartition,Long]()
    //获取jedis的连接
    val jedis = MyJedisPool.getConnection()
    //查询redis的所有kafka相关的topic和partiton
    val topicpartiotionoffset : util.Map[String, String] = jedis.hgetAll(groupId)
    //导入隐式转换
    import  scala.collection.JavaConversions._
    //将topicpartitonoffset转换成list处理
    val list: List[(String, String)] = topicpartiotionoffset.toList
    //循环处理数据
    for (topic <- list) {
      formdbOffset += (
      new TopicPartition(topic._1.split("[-]")(0),topic._1.split("[-]")(1).toInt) -> topic._2.toLong)
    }
    formdbOffset
  }
}
