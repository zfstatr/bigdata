package org.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        //1.实例化kafka集群
        Properties properties = new Properties();
        properties.load(Producer.class.getClassLoader().getResourceAsStream("producer1.properties"));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //2.用集群对象发送数据
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(
                    //封装producRecord
                    new ProducerRecord<String, String>(
                            "first",
                            Integer.toString(i),
                            "Value" + i
                    ),
                    //回调函数
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println(metadata);
                            }
                        }
                    }
            );
//            RecordMetadata recordMetadata = future.get();
            System.out.println("发完了" + i + "条");

        }
        
        //3.关闭资源
        producer.close();
    }
}
