package com.atguigu.gmall0218.realtime.bean

case class  AlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)  {

}

