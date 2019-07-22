package com.atguigu.gmall2018.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall2018.canal.handler.CanalHanlder;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        // 创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
        while (true){
            //连接
            canalConnector.connect();
            // 抓取的表
            canalConnector.subscribe("gmall0218.*");
            // 抓取
            Message message = canalConnector.get(100);
            if(message.getEntries().size()==0){
                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //每个entry对应一个sql
                    //过滤一下entry 因为不是每个sql都是对数据进行修改的写操作 比如 开关事务
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange=null;
                        try {
                            //把storeValue进行反序列化 得到rowChange
                            rowChange= CanalEntry.RowChange.parseFrom(entry.getStoreValue())     ;
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();  //行集
                        CanalEntry.EventType eventType = rowChange.getEventType();// insert  update delete drop alter
                        String tableName = entry.getHeader().getTableName();  //表名

                        CanalHanlder canalHanlder =new CanalHanlder(tableName,eventType,rowDatasList);
                        canalHanlder.handle();
                    }
                }


            }


        }


    }
}
