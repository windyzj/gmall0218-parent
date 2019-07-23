package com.atguigu.gmall0218.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0218.common.constant.GmallConstant
import com.atguigu.gmall0218.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0218.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {


  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_info")

      val ssc = new StreamingContext(sparkConf,Seconds(5))

      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)
    // 0 转换为 样例类
    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      eventInfo
    }


//    1  同一设备  ---> 分组  groupby
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()


//      2  5分钟内 ( 课堂演示统计30秒)    滑动窗口  窗口大小  滑动步长

    val windowDstream: DStream[(String, Iterable[EventInfo])] = groupbyMidDstream.window(Seconds(30),Seconds(5))



//    3  三次及以上用不同账号登录并领取优惠劵
//      4  领劵过程中没有浏览商品
    val checkAlertInfoDstream: DStream[(Boolean, AlertInfo)] = windowDstream.map { case (mid, eventInfoItr) =>

      val couponUidsSet = new util.HashSet[String]()
      val itemIdsSet = new util.HashSet[String]() //商品ID
    val eventInfoList = new util.ArrayList[String]()
      var clickItemFlag: Boolean = false
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventInfoList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") {
            couponUidsSet.add(eventInfo.uid)
            itemIdsSet.add(eventInfo.itemid)
          } else if (eventInfo.evid == "clickItem") {
            clickItemFlag = true;
            break
          }
        }
      )
      val alertInfo = AlertInfo(mid, couponUidsSet, itemIdsSet, eventInfoList, System.currentTimeMillis())
      (couponUidsSet.size() >= 3 && !clickItemFlag, alertInfo)

    }
    checkAlertInfoDstream.foreachRDD{rdd=>
      println(rdd.collect().mkString("\n"))

    }
    
    
    
    
    
//      5  同一设备，每分钟只记录一次预警。去重


    ssc.start()
    ssc.awaitTermination()

  }
}
