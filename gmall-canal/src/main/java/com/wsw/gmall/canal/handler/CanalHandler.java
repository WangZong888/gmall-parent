package com.wsw.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wsw.gamll.common.constant.ConstantClass;
import com.wsw.gmall.canal.MyKafkaSender;

import java.util.List;
import java.util.Random;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-23 11:41
 **/
public class CanalHandler {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT) {//下单操作
            sendKafka(ConstantClass.KAFKA_TOPIC_ORDER);
        } else if (tableName.equals("order_detail") && eventType == CanalEntry.EventType.INSERT) {//订单明细
            sendKafka(ConstantClass.KAFKA_TOPIC_ORDER_DETAIL);
        } else if (tableName.equals("user_info") && eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
            sendKafka(ConstantClass.KAFKA_TOPIC_USER);//用户信息
        }


    }

    public void sendKafka(String topic) {

        for (CanalEntry.RowData rowData : rowDataList) {//遍历行集
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();//修改后的列集
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {//遍历列集
                System.out.println(column.getName() + "-----" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            try {
                Thread.sleep(new Random().nextInt(5)*1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            MyKafkaSender.send(topic, jsonObject.toJSONString());//发送卡夫卡

        }

    }
}
