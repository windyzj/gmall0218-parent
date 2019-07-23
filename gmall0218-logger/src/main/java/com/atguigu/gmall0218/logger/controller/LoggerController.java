package com.atguigu.gmall0218.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0218.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController   //controller +    @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @GetMapping("test")
    public  String getTest(){
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!1");
        return "success";
    }

    @PostMapping("log")
    public  String doLog(@RequestParam("logString") String logString){

        // 1 加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String jsonString = jsonObject.toJSONString();
        //2     log4j logback
        log.info(jsonString);


        //3 发送kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }


        return "success";

    }

}
