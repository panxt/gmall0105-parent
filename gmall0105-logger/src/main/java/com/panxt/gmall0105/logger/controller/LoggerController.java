package com.panxt.gmall0105.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import  com.panxt.gmall.constant.GmallConstant;

/**
 * @author panxt
 * @create 2019-06-25 17:40
 */
@RestController //==> == Controller + responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public String doLog(@RequestParam("log") String log){

        // 0.补充时间戳
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",System.currentTimeMillis());

        // 1.落盘
        String jsonString = jsonObject.toJSONString();
        logger.info(jsonObject.toJSONString());

        // 2.推送到kafka
        if("startup".equals(jsonObject.get("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }
}
