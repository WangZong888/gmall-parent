package com.wsw.gmall.realtime

import java.util

import com.alibaba.fastjson.JSON
import com.wsw.gamll.common.constant.ConstantClass
import com.wsw.gmall.bean.OrderInfo
import com.wsw.gmall.sparkutils.{MykafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object OrderApp {

  def main(args: Array[String]): Unit = {

    //配置环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //消费kafka
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(ConstantClass.KAFKA_TOPIC_ORDER, ssc)

    //对数据进行转化
    val orderInfoDStream: DStream[OrderInfo] = kafkaMessage.map(recoder => {
      //1.拿到数据V
      val jsonString: String = recoder.value()
      //2.添加类型
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //3.补充日期字段
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      //日期
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      //小时
      //4.对电话号码脱敏——>如 1234*******
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"

      orderInfo
    })

//    println("------"+orderInfoDStream)
   val transRDD: DStream[OrderInfo] = orderInfoDStream.transform(rdd => {
      //4.增加一个字段在订单表中，标识该订单表是否是该用户的首次下单，“1”表示是首次下单
      //"0"非首次下单，
      //a.维护一个清单 每次新用户下单，要追加入清单
      val client: Jedis = RedisUtil.getJedisClient
      val userIds: util.Set[String] = client.smembers("userIdFlag")
      client.close()
      val useridBroad: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(userIds)
      val mapRDD: RDD[OrderInfo] = rdd.map(action => {
        if (!useridBroad.value.contains(action.user_id)) {
          val client1: Jedis = RedisUtil.getJedisClient
          //c.添加到redis中辅助标识
          client1.sadd("userIdFlag", action.user_id)
          client1.close()
          //b.利用清单进行标识
          action.order_flag = 0
        }
        action
      })
      mapRDD
    })

    transRDD.print()
    //本批次内过滤同时还要查询本批次——根据userid进行分组，并按时间戳进行排序
    val listOrderInfo: DStream[List[OrderInfo]] = transRDD.map(rdd => (rdd.user_id, rdd)).groupByKey().mapValues(_.toList.sortWith(_.create_time < _.create_time)).map(_._2)

    val flagOrderInfo: DStream[OrderInfo] = listOrderInfo.flatMap(list => {
      if (list(0).order_flag == 0) {
        list(0).order_flag = 1;
      } else {
        for (elem <- list) {
          elem.order_flag = 0;
        }
      }
      list
    })



    //保存到HBASE+Phoenix中
    flagOrderInfo.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL0218_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR", "ORDER_FLAG"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    //开启ssc
    ssc.start()
    // driver等待ssc
    ssc.awaitTermination()
  }

}
