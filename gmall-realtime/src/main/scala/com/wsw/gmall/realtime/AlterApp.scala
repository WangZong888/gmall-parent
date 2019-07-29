package com.wsw.gmall.realtime

import java.util

import com.alibaba.fastjson.JSON
import com.wsw.gamll.common.constant.ConstantClass
import com.wsw.gmall.bean.{AlertInfo, EventInfo}
import com.wsw.gmall.sparkutils.{MyEsUtil, MykafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp {

  /**
    * 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。
    * 达到以上要求则产生一条预警日志。同一设备，每分钟只记录一次预警
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("alter-app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //从kafka获取数据
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(ConstantClass.KAFKA_TOPIC_EVENT, ssc)

    //转换成样例类
    val eventInfoDStream: DStream[EventInfo] = inputDStream.map(record => {
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      eventInfo
    })
    //分析思路
    //1.同一设备
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoDStream.map(t => (t.mid, t)).groupByKey()
    //2. 5分钟内，滑动窗口， 窗口大小，步长
    val windowDStream: DStream[(String, Iterable[EventInfo])] = groupbyMidDstream.window(Seconds(30), Seconds(5))

    //打标记 是否满足预警条件 整理成预警格式
    //3. 三次及以上使用不同账号登录并领取优惠券
    //4. 领券过程中，没有浏览商品
    val checkAlertInfoDstream: DStream[(Boolean, AlertInfo)] = windowDStream.map {
      case (mid, eventInfoIter) => {
        val couponUidsSet = new util.HashSet[String]()
        val itemIdSets = new util.HashSet[String]()// 商品id
        val eventInfoList = new util.ArrayList[String]()
        //存放行为id，行为id有重复的
        var clickItemFlag: Boolean = false //标识是否点击过商品
        breakable(for (eventInfo <- eventInfoIter) {
          eventInfoList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") {
            couponUidsSet.add(eventInfo.uid)
            itemIdSets.add(eventInfo.itemid)
          } else if (eventInfo.evid == "clickItem") {
            //只要点击，这个设备就不计算在预警中
            clickItemFlag = true
            break
          }
        })
        val alertInfo = AlertInfo(mid, couponUidsSet, itemIdSets, eventInfoList, System.currentTimeMillis())
        (couponUidsSet.size() >= 3 && !clickItemFlag, alertInfo)
      }
    }

//    checkAlertInfoDstream.foreachRDD(rdd=>{
//      println(rdd.collect().mkString("\n"))
//    })

    //过滤不满足预警条件的数据
    val alretInfoDStream: DStream[AlertInfo] = checkAlertInfoDstream.filter(_._1).map(_._2)
    //5. 同一设备，每分钟只记录一次预警， 去重
    //方法：可以依靠存储的容器去重， 主键 在MySQL中插入相同的主键会报错，但是在esearch和Phoenix中后面的会覆盖前面的
    //策略：利用es的主键进行去重 主键由 mid+分钟 组成 确保每一分钟相同的mid只能有一条预警



    //6.保存到ES中
    alretInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(alertInfo=>{
          //主键id+分钟 组成
        val alertList: List[(String, AlertInfo)] = alertInfo.toList.map(alertinfo=>(alertinfo.mid+"_"+alertinfo.ts/1000/60,alertinfo))
        println(alertList.mkString("\n"))

        MyEsUtil.insertBulk(ConstantClass.ES_INDEX_ALERT,alertList)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
