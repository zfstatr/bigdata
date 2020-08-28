package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.producer.Producer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) throws IOException, InterruptedException {
        //1.实例化consumer对象
        Properties properties = new Properties();
        properties.load(Consumer.class.getClassLoader().getResourceAsStream("consumer1.properties"));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //2.接收消息

        //订阅话题的消息
        consumer.subscribe(Collections.singleton("first"));

        while (true){

            //从话题中拉去数据
            ConsumerRecords<String, String> poll = consumer.poll(200);
            if (poll.count() == 0){
                Thread.sleep(100);
            }
            //消费拉取的数据
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }
//            consumer.commitAsync();
        }
        //3.关闭资源
//        consumer.close();
    }
}
