package com.atguigu.gmall0218.realtime.bean

import java.text.SimpleDateFormat
import java.util



case class SaleDetail(
                       var order_detail_id:String =null,
                       var order_id: String=null,
                       var order_status:String=null,
                       var create_time:String=null,
                       var user_id: String=null,
                       var sku_id: String=null,
                       var user_gender: String=null,
                       var user_age: Int=0,
                       var user_level: String=null,
                       var sku_price: Double=0D,
                       var sku_name: String=null,
                       var dt:String=null)
{
  def this(orderInfo:OrderInfo,orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo:OrderInfo): Unit ={
    if(orderInfo!=null){
      this.order_id=orderInfo.id
      this.order_status=orderInfo.order_status
      this.create_time=orderInfo.create_time
      this.dt=orderInfo.create_date
      this.user_id=orderInfo.user_id
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit ={
    if(orderDetail!=null){
      this.order_detail_id=orderDetail.id
      this.sku_id=orderDetail.sku_id
      this.sku_name=orderDetail.sku_name
      this.sku_price=orderDetail.order_price.toDouble


    }
  }

  def mergeUserInfo(userInfo: UserInfo): Unit ={
    if(userInfo!=null){
      this.user_id=userInfo.id

      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: util.Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val  betweenMs= curTs-date.getTime
      val age=betweenMs/1000L/60L/60L/24L/365L

      this.user_age=  age.toInt
      this.user_gender=userInfo.gender
      this.user_level=userInfo.user_level

    }
  }



}

