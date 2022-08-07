package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author chulang
 * @date 2022/8/6 03:42
 * @description 带回调函数带异步发送分区
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) throws InterruptedException {

        // 0 配置
        Properties properties = new Properties();
        // 连接kafka配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.142.183.27:9092");
        // 指定key value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 关联自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.kafka.producer.MyPartitioner");

        // 1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2.发送数据
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord<>("test0805","hello kafka atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + " 分区："+recordMetadata.partition());
                    }
                }
            });

            Thread.sleep(2);
        }

        // 3 关闭资源
        kafkaProducer.close();

        // 在kafka服务端执行消费，可以看到数据
        //kafka-console-consumer.sh --bootstrap-server 43.142.183.27:9092 --topic test0805 --from-beginning
    }
}
