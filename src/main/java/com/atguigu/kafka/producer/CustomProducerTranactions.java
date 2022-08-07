package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author chulang
 * @date 2022/8/6 03:42
 * @description 简单异步发送数据
 */
public class CustomProducerTranactions {
    public static void main(String[] args) {

        // 0 配置
        Properties properties = new Properties();

        // 连接集群

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.142.183.27:9092");

        // 指定key value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //指定事务id，全局唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"tra_id_01");

        // 1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 事务相关
        kafkaProducer.initTransactions();

        kafkaProducer.beginTransaction();

        try {
            // 2.发送数据
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("test0805", "hello kafka" + i));
            }

            // 测试事务出错数据发送不成功
//            int i = 1/0;

            kafkaProducer.commitTransaction();

        } catch (Exception e) {
            kafkaProducer.abortTransaction();

        } finally {
            // 3 关闭资源
            kafkaProducer.close();

        }


        // 在kafka服务端执行消费，可以看到数据
        //kafka-console-consumer.sh --bootstrap-server 43.142.183.27:9092 --topic test0805 --from-beginning
    }
}
