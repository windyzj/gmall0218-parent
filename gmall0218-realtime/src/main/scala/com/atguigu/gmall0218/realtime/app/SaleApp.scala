package com.atguigu.gmall0218.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0218.common.constant.GmallConstant
import com.atguigu.gmall0218.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall0218.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sale_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputOrderDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //整理 转换
    val orderInfoDstream: DStream[OrderInfo] = inputOrderDstream.map { record =>
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
      orderInfo
    }


    val orderDetailDStream: DStream[OrderDetail] = inputOrderDetailDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }
    // 双流join 前 要把流变为kv 结构
    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo =>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    //为了不管是否能够关联左右 ，都要保留左右两边的数据 采用full join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[SaleDetail] = fullJoinDStream.mapPartitions { partitionItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      implicit val formats = org.json4s.DefaultFormats
      val saleDetailList = ListBuffer[SaleDetail]()
      for ((orderId, (orderInfoOption, orderDetailOption)) <- partitionItr) {
        if (orderInfoOption != None) {
          println(" 主表有数据 ！")
          val orderInfo: OrderInfo = orderInfoOption.get
          // 1 组合关联
          if (orderDetailOption != None) {
            println(" 主表有数据 ！且从表有数据 成功关联")
            val orderDetail: OrderDetail = orderDetailOption.get
            //组合成一个SaleDetail
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            // 存放到sale集合中
            saleDetailList += saleDetail
          }

          //2  写缓存  key 类型 : string   key名 [order_info:order_id]  value -> orderinfoJson
          println(" 主表有数据 ！写入缓存")
          val orderInfoKey = "order_info:" + orderId

          // fastjson无法转换 case class 为json
          //  val orderInfoJson: String = JSON.toJSONString(orderInfo)
          // json4s
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.setex(orderInfoKey, 300, orderInfoJson)

          // 3 查询缓存
          val orderDetailKey = "order_detail:" + orderId
          val orderDetailJson: String = jedis.get(orderDetailKey)
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
          import collection.JavaConversions._
          for ( orderDetailJson <- orderDetailSet ) {
            println(" 查询到从表缓存数据进行关联")
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        } else if (orderDetailOption != None) { //主表没有数据 从表有数据
          println("主表没有数据 从表有数据 ")
          val orderDetail: OrderDetail = orderDetailOption.get
          //1 查询缓存  查询主表
          println("查询主表缓存")
          val orderInfoKey = "order_info:" + orderId
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (orderInfoJson != null && orderInfoJson.size > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
          // 2 从表写缓存   // 从表缓存设计问题 //要体现一个主表下多个从表的结构1：n    keytype: set  key order_detail:order_id  members -> 多个 order_detailjson
          println("写从表缓存")
          val orderDetailKey = "order_detail:" + orderId
          val orderDetailJson: String = Serialization.write(orderDetail)
          jedis.sadd(orderDetailKey,orderDetailJson)
          jedis.expire(orderDetailKey,300)
          //jedis.setex(orderDetailKey, 300, orderDetailJson)

        }
      }
      jedis.close()
      saleDetailList.toIterator
    }


    saleDetailDstream.foreachRDD{rdd=>
      println(rdd.collect().mkString("\n"))
    }



   ssc.start()
    ssc.awaitTermination()

  }

}
