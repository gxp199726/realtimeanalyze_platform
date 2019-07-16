package cn.qf.service

import java.lang
import cn.qf.utils.{CaculateTool, MyJedisOffset, MyJedisPool}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 直连方式，将offset保存到redis中
  */
object kafkaRedisStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(3))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "test2"
    // topic
    val topic = "hz1803"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.153.200:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = MyJedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    val value = ssc.sparkContext.textFile("F:\\bigdata\\HZ1803\\项目（二）01\\充值平台实时统计分析\\city.txt")
      .map(t=>(t.split(" ")(0),t.split(" ")(1)))
    val broadcasts = ssc.sparkContext.broadcast(value.collect().toMap)
    // 处理数据
    stream.foreachRDD(rdd=>{
      //首先我们想获取处理数据的全信息，包括topic partition、offset
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 业务处理
      val baseData = rdd.map(_.value()).map(t=>JSON.parseObject(t))
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(t=>{
          // 判断充值结果
          val result = t.getString("bussinessRst") // 充值结果
          val fee :Double = if(result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
          val startTime = t.getString("RequestId")// 开始充值时间
          //数据当前日期
          val day = startTime.substring(0,8)
          val hour = startTime.substring(8,10)
          val minute = startTime.substring(10,12)
          val endTime = t.getString("receiveNotifyTime") // 结束充值时间
          val pcode = t.getString("provinceCode") // 获得省份编号
          val city = broadcasts.value.get(pcode).toString // 通过省份编号进行取值
          val isSucc  = if(result.equals("0000")) 1 else 0 // 充值成功数
          // 充值时长
          val costtime :Long = if(result.equals("0000")) CaculateTool.caculateTime(startTime,endTime) else 0
          //if(result.equals("0000"))
          (day,hour,List[Double](1,isSucc,fee,costtime),city,minute)
          })
         // 指标1
      JedisKpi.Result01(baseData)



              // 更新偏移量
              val jedis = MyJedisPool.getConnection()
              // 获取offset信息
              for(or <- offsetRange){
                jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
              }
              jedis.close()
            })
          // 启动程序
          ssc.start()
          ssc.awaitTermination()
        }
    }
