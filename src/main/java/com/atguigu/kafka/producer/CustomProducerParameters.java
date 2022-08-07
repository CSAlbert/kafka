package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author chulang
 * @date 2022/8/6 10:49
 * @description TODO
 */
public class CustomProducerParameters {

    public static void main(String[] args) {

        //0 配置
        Properties properties = new Properties();
        // 必要参数（kafka地址、序列化）
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.142.183.27:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 提高吞吐量优化型参数（批次大小、linger.ms、压缩）
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 1 创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2 发送数据
        for (int i = 1; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("test0805", "kafka msg " + i));
        }

        // 3 关闭资源
        kafkaProducer.close();
    }

}
