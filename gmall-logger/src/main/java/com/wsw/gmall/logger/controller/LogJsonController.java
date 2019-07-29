package com.wsw.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import com.wsw.gamll.common.constant.ConstantClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-20 08:33
 **/
@Slf4j
@RestController
public class LogJsonController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    @PostMapping("log")
    public String doLong(@RequestParam("logString")String logString) {

        //0.补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //1.落盘file
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        //2.推送到kafka--卡夫卡没有消费的话，控制台打印就很慢
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(ConstantClass.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(ConstantClass.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }
}
