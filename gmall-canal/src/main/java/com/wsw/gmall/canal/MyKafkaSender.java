package com.wsw.gmall.canal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-23 11:54
 **/
public class MyKafkaSender {

    public static KafkaProducer<String,String> kafkaProducer = null;

    public static KafkaProducer<String, String> createKafkaproducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //properties.put("zookeeper.connect", "hadoop102:2181");
        properties.put("key.serializer", "org.apache.kafka.sendKafka.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.sendKafka.serialization.StringSerializer");

        KafkaProducer<String,String> producer = null;

        try {
            Thread.currentThread().setContextClassLoader(null);
            producer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaproducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic,msg));
    }
}
