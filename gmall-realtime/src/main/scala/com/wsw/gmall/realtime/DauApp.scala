package com.wsw.gmall.realtime

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wsw.gamll.common.constant.ConstantClass
import com.wsw.gmall.bean.Startuplog
import com.wsw.gmall.sparkutils.{MykafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.Seq

object DauApp {

  def main(args: Array[String]): Unit = {

    //TODO 需求，求每日的活跃用户
    //环境配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf,Seconds(5))
    //TODO 1.消费kafka——实时处理-sparkStreaming
    //根据Kafka工具获取数据、
    val topic : String = ConstantClass.KAFKA_TOPIC_STARTUP
    val kafkaStartupDatas: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(topic,ssc)

    //验证kafka数据
//    kafkaStartupDatas.foreachRDD(rdd =>{
//      println(rdd.map(_.value()).collect().mkString("\n"))
//    })
    //对数据进行结构转换case class 补充2个时间字段
    val startupCaseDStream: DStream[Startuplog] = kafkaStartupDatas.map(data => {
      val jsonString: String = data.value()
      val startuplog: Startuplog = JSON.parseObject(jsonString, classOf[Startuplog])
      val formatDate = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHours: String = formatDate.format(new Date(startuplog.ts))
      val dateArray: Array[String] = dateHours.split(" ")
      startuplog.logDate = dateArray(0)
      startuplog.logHour = dateArray(1)
      startuplog
    })
    startupCaseDStream.cache()//将多次用到的数据进行缓存

    //TODO 2.去重，根据今天访问过得用户清单进行过滤--使用redis来进行去重
    //因为Drier只启动一次，所以需要transform
    //这是批次之间的去重
    val filterDStream: DStream[Startuplog] = startupCaseDStream.transform(rdds => {
      println("过滤前："+rdds.count())
      val client: Jedis = RedisUtil.getJedisClient
      //当前日期
      val formatDate = new SimpleDateFormat("yyyy-MM-dd")
      val dateCurr: String = formatDate.format(new Date())
      val key = "dau:" + dateCurr
      //println("key1:"+key)
      val midSet: util.Set[String] = client.smembers(key)
      client.close()
      //使用broadcast--》Driver->Executor
      val broadMid: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      val value: RDD[Startuplog] = rdds.filter(rdd => {
        !broadMid.value.contains(rdd.mid)
      })
      println("过滤后："+value.count())
      value
    })


    //本批次内去重-按mid分组，取每组第一个人
    val distinctDStream: DStream[Startuplog] = filterDStream.map(data => (data.mid, data)).groupByKey().flatMap(_._2.take(1))

    //TODO 3.把所有今天访问过得用户保存到redis中——目的是为了辅助过滤
    distinctDStream.foreachRDD(data =>{
      data.foreachPartition(rdds=>{//减少创建连接的次数
        val client: Jedis = RedisUtil.getJedisClient
        for(rdd <- rdds) {
          val key = "dau:"+rdd.logDate//将登录时间作为key
          //println("key2:"+key)
          client.sadd(key,rdd.mid)
          println(rdd)
        }
        client.close()
      })
    })
    //TODO 4.保存到HBase中--使用Phoenix来做中间过程
    //导入隐式转换
    import org.apache.phoenix.spark._
    distinctDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("gmall0218_dau",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //记得启动streaming和Driver等待
    ssc.start()
    ssc.awaitTermination()
  }

}
