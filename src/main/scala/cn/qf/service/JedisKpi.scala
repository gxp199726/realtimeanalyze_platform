package cn.qf.service

import cn.qf.utils.MyJedisPool
import org.apache.spark.rdd.RDD

object JedisKpi {
  /**
    * 统计全网的充值订单量, 充值金额, 充值成功数,统计充值总时长。
    * @param lines
    */
 def Result01(baseData:RDD[(String,String,List[Double],String,String)]): Unit = {
   baseData.map(t=>(t._1,t._3)).reduceByKey((list1,list2)=>{
     //将所有的元素拉链为一个列表之后进行相加
     list1.zip(list2).map(t=>t._1+t._2)
   }).foreachPartition(f=>{
     val jedis = MyJedisPool.getConnection()
     f.foreach(t=>{
       //充值订单数
       jedis.hincrBy(t._1,"count",t._2(0).toLong)
       //充值金额
       jedis.hincrByFloat(t._1,"money",t._2(2))
       //充值成功数
       jedis.hincrBy(t._1,"success",t._2(1).toLong)
      //统计总时长
       jedis.hincrBy(t._1,"cost",t._2(3).toLong)
     })
     jedis.close()
   })
 }

  /**
    * 全网每分钟的订单量数据
    */
  def Result02(baseData:RDD[(String,String,List[Double],String,String)]):Unit ={
    baseData.map(t=>((t._1,t._2,t._5),List(t._3(0)))).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreachPartition(f=>{
      val jedis = MyJedisPool.getConnection()
      f.foreach(t=>{
        jedis.hincrBy(t._1._1,"count/m"+t._1._2+t._1._3,t._2(0).toLong)
      })
      jedis.close()
    })
  }

  /**
    * 实时统计每分钟的充值笔数和充值金额
    */
  def Result05(baseData:RDD[(String,String,List[Double],String,String)]) :Unit = {
    baseData.map(t=>((t._1,t._2,t._5),List(t._3(1),t._3(2)))).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreachPartition(f=>{
      val jedis = MyJedisPool.getConnection()
      f.foreach(t=>{
        //每分钟的充值笔数和充值金额
        jedis.hincrByFloat(t._1._1,"M"+t._1._2+t._1._3,t._2(0))
        jedis.hincrBy(t._1._1,"money/m"+t._1._2+t._1._3,t._2(0).toLong)
      })
      jedis.close()
    })
  }
}
