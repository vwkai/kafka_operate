package com.vwkai.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerDatas {
    private static final String ERROR_FILE_NAME = "/tmp/log/producerDataErrors.log";
    private static final String TOPIC = "test";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");

        //消息的确认机制 0 1 -1或all
        props.put("acks", "all");
        props.put("retries", 0);
        //缓冲区的大小  //默认32M
        props.put("buffer.memory", 33554432);
        //批处理数据的大小，每次写入多少数据到topic   //默认16KB
        props.put("batch.size", 16384);
        //可以延长多久发送数据   //默认为0 表示不等待 ，立即发送
        props.put("linger.ms", 1);
        //指定数据序列化和反序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义partition
        props.put("partitioner.class", "com.vwkai.kafka.MyPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int messageNo = 1;
        while (true) {
            String message = (new KafkaProducerDatas()).getSendMessage(messageNo);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, message);

            // 异步写入
            producer.send(producerRecord, new DataCallBack(TOPIC, message));

            messageNo++;
            Thread.sleep(2000);
        }
    }

    private String getSendMessage(int messageNo) {
        return "hello world " + messageNo;
    }

    public static void WriteFile(String content) {
        try {
            File file = new File(ERROR_FILE_NAME);

            //if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            //true = append file
            FileWriter fileWritter = new FileWriter(file.getPath(), true);
            fileWritter.write(content);
            fileWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class DataCallBack implements Callback {
    private final String topic;
    private final String message;

    public DataCallBack(String topic, String message) {
        this.topic = topic;
        this.message = message;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception != null) {
            System.out.println("有异常，需要重新处理");
            //将失败数据写入文件
            KafkaProducerDatas.WriteFile("topic:" + this.topic + " | message:" + this.message + "\n");
        } else {
            System.out.println("没有异常，正常发送数据");
        }
        if (recordMetadata != null) {
            System.out.println("topic:" + recordMetadata.topic() + " | partition: " + recordMetadata.partition() + " | offset:" + recordMetadata.offset());
        }
    }
}
