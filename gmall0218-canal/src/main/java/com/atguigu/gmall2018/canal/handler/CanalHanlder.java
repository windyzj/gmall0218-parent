package com.atguigu.gmall2018.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0218.common.constant.GmallConstant;
import com.atguigu.gmall2018.canal.MyKafkaSender;


import java.util.List;

public class CanalHanlder {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHanlder(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT){ // 下单操作
            for (CanalEntry.RowData rowData : rowDataList) {  //遍历行集
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList(); //修改后的列集
                JSONObject jsonObject=new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {  //遍历列集
                    System.out.println( column.getName()+"|||||"+column.getValue());
                    jsonObject.put( column.getName(),column.getValue());
                }
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString()); //发送kafka

            }
        }


    }



}
