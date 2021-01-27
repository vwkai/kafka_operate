package com.vwkai.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumPartition {
    public static void main(String[] args) {
        Properties props= new Properties();
        props.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        props.put("group.id", "test-tp111");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        String topic ="test";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(3));
            for(ConsumerRecord<String, String> record : records)
                System.out.printf("partition=%d, offset= %d, key = %s, value = %s%n",record.partition(),record.offset(), record.key(),record.value());
        }
    }
}
