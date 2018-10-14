package golan.izik.consumer;

import golan.izik.mng.CmdOpts;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerWithOffset {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithOffset.class);

    private static final String CLIENT_ID         = "ClientId_Izik";

    private static final String CLA_KAFKA         = "kafka";
    private static final String CLA_TOPIC         = "topic";
    private static final String CLA_PARTITION     = "partition";
    private static final String CLA_BEGIN         = "begin";
    private static final String DEFAULT_PARTITION = "0";
    private static final String DEFAULT_BEGIN     = "0";


    private static CmdOpts opts = null;


    public static void main(String[] args) throws ParseException {
        opts = new CmdOpts(args, getCommandLineArguments(), Collections.emptySet());

        log.info("Starting Kafka Consumer...\n" + opts);
        fetchFromOffset(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)), Long.parseLong(opts.get(CLA_BEGIN)));
    }

    private static void fetchFromOffset(final String topic, final int partition, final long offset) {
        Properties props = new Properties();
        props.put("bootstrap.servers", opts.get(CLA_KAFKA));
        props.put("group.id", CLIENT_ID);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            final TopicPartition topicPartition = new TopicPartition(topic, partition);

            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    consumer.seek(topicPartition, offset);
                }
            });
            ConsumerRecords<String, String> data = consumer.poll(15000);
            System.out.println("Found ["+data.count()+"] messages from offset ["+offset+"]");
            System.out.println("assignment: " + Arrays.toString(consumer.assignment().toArray()));
//            for (ConsumerRecord<String, String> record : data.records(topicPartition)) {
//
//            }

        }
        catch (Exception e) {
            log.error("Failed in fetchFromOffset", e);
        }







    }

    private static Map<String, String> getCommandLineArguments() {
        Map<String, String> result = new HashMap<>();
        result.put(CLA_KAFKA, "localhost:19092");
        result.put(CLA_TOPIC, "izik-test-2");
        result.put(CLA_PARTITION, DEFAULT_PARTITION);
        result.put(CLA_BEGIN, DEFAULT_BEGIN);
        return result;
    }

}
