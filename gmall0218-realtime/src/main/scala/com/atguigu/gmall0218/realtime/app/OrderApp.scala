package com.atguigu.gmall0218.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0218.common.constant.GmallConstant
import com.atguigu.gmall0218.realtime.bean.OrderInfo
import com.atguigu.gmall0218.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    //整理 转换
    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      // 1 转换成case class
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      // 2  脱敏 电话号码  1381*******
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"
      // 3  补充日期字段
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0) //日期

      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0) //小时

      // 4 练习需求  增加一个字段 在订单表 ，  标识该笔订单是否是该用户的首次下单， “1”表示 首次下单， “0”表示非首次下单
      // 1 维护一个清单 每次新用户下单 要追加入清单  2 利用清单进行标识 3 同时还要查询本批次    //要注意状态信息表的大小 来决定存储容器  redis<mysql<es< hbase

      orderInfo
    }

    // 保存到hbase + phoenix
    orderInfoDstream.foreachRDD{rdd=>

      rdd.saveToPhoenix("GMALL0218_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,
        Some("hadoop1,hadoop2,hadoop3:2181") )

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
