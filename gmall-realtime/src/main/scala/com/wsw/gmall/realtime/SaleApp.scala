package com.wsw.gmall.realtime


import java.util

import com.alibaba.fastjson.JSON
import com.wsw.gamll.common.constant.ConstantClass
import com.wsw.gmall.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.wsw.gmall.sparkutils.{MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

object SaleApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("saleapp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderDS: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(ConstantClass.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDS: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(ConstantClass.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //对数据进行转换
    val orderInfoDStream: DStream[OrderInfo] = orderDS.map(recoder => {
      //拿到数据V
      val stringJson: String = recoder.value()
      //将数据添加到样例类中
      val orderInfo: OrderInfo = JSON.parseObject(stringJson, classOf[OrderInfo])
      //补充日期字段
      val dataTime: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dataTime(0)
      val hourString: Array[String] = dataTime(1).split(":")
      orderInfo.create_hour = hourString(0)
      //对电话号码脱敏->1234*******
      val tel: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tel._1 + "*******"
      orderInfo
    })

    val orderDetailDStream: DStream[OrderDetail] = orderDetailDS.map(recoder => {
      val jsonString: String = recoder.value()
      val detail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      detail
    })
    //双流join 前，要把流变成K—V结构
    val orderInfoWithKeyDS: DStream[(String, OrderInfo)] = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDS: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //为了不管是否能够关联左右，都要保留左右两边的数据，采用 full join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDS.fullOuterJoin(orderDetailWithKeyDS)
    val saleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partitionItr => {
      implicit val formats = org.json4s.DefaultFormats
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailList = scala.collection.mutable.ListBuffer[SaleDetail]() //存放saleDetail
      for ((orderId, (orderInfoOpation, orderDetailOptation)) <- partitionItr) {
        if (orderInfoOpation != None) { //主表有数据
          println("主表有数据！")
          //组合成一个SaleDetail,去掉Some (get)
          val orderInfo = orderInfoOpation.get
          //TODO 1.将数据组合到SaleDetail表中
          if (orderDetailOptation != None) { //从表有数据
            println("从表有数据！且成功关联 ")
            val orderDetail = orderDetailOptation.get
            val saledetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saledetail
          }
          /*//TODO 2.将数据添加到主表缓存中
          //写缓存，key 类型:String  key名[order_info:order_id] value -> orderinfojson
          println("主表有数据！写入缓存")
          val orderInfoKey = "order_info:" + orderId
          //JSON不能解析Scala
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.setex(orderInfoKey, 300, orderInfoJson)
          //TODO 3.还要看看是否在从表的缓存中是否有对应的
          val orderDetailKey = "order_detail:" + orderId
          //val orderDetailJson: String = jedis.get(orderDetailKey)
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
          import  collection.JavaConversions._ //作用是将java集合做隐式转换
          for(orderDetailJson <- orderDetailSet){
            if (orderDetailJson != null && orderDetailJson.size > 0) {
              println("主表有数据！查询到从表的缓存数据进行关联")
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saledetail = new SaleDetail(orderInfo, orderDetail)
              saleDetailList += saledetail //存放到集合中
            }
          }*/

        } /*else if (orderDetailOptation != None) {
          //主表没有数据，从表有数据
          //TODO 1.查询缓存 查询主表
          val orderDetail = orderDetailOptation.get
          val orderInfoKey = "order_info:" + orderId
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (orderInfoJson != null && orderInfoJson.size > 0) {
            println("主表没有有数据，从表有数据！从表与主表缓存进行关联")
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saledetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saledetail //存放到集合中
          }
          //TODO 2.从表写缓存 从表设计得注意可能有多个相同的key /要体现一个主表下多个从表的结构1:n
          //TODO keytype:set key->order_detail:order_id  members->多个order_detailjson
          println("主表没有有数据，从表有数据！从表写入缓存")
          val orderDetailKey = "order_detail:" + orderId
          //JSON不能解析Scala
          val orderDetailJson: String = Serialization.write(orderDetailKey)
          //jedis.setex(orderDetailKey, 300, orderDetailJson)
          jedis.sadd(orderDetailKey,orderDetailJson)
          jedis.expire(orderDetailKey,300)
        }*/
      }
      jedis.close()
      saleDetailList.toIterator
    })

    saleDetailDStream.foreachRDD(rdd=>
    println(rdd.collect().mkString("\n")))

    //streaming记得启动与等待
    ssc.start()
    ssc.awaitTermination()
  }
}
