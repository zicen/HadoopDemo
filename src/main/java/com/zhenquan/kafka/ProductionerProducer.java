package com.zhenquan.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProductionerProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "192.168.195.12:9092");
        props.put("partitioner.class", "com.zhenquan.kafka.SimplePartitoner");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        String toppic = "lxw1234";
        for (int i = 0; i < 11; i++) {
            String k = "key" + i;
            String v = k + "--value" + i;
            producer.send(new KeyedMessage<>(toppic, k, v));
        }
        producer.close();
    }
}
