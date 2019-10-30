package golan.izik.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class HighLevelConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("data-in-izik"));
        try {
            for (int i = 0; i < 2; i++) {
                System.out.println("====== ["+i+"]");
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println("*** OFFSET:" + record.offset() + " VALUE: " + record.value());
            }
        } finally {
            System.out.println("====== CLOSE");
            consumer.close();
        }

    }
}
