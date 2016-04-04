package golan.izik.consumer;

import golan.izik.mng.CmdOpts;
import golan.izik.mng.Utils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerWithOffset {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithOffset.class);

    public static final String CLIENT_ID = "ClientId_Izik";
    public static final String TOPIC = "test1";
    public static final int PARTITION = 0;
    public static final int CHUNK_SIZE = 10000;

    public static final String CLA_KAFKA     = "kafka";
    public static final String CLA_TOPIC     = "topic";
    public static final String CLA_PARTITION = "partition";
    public static final String CLA_BEGIN     = "begin";
    public static final String CLA_MSG_COUNT = "msg_count";

    public static final String DEFAULT_KAFKA     = "localhost:9092";
    public static final String DEFAULT_TOPIC     = "data-in-build";
    public static final String DEFAULT_PARTITION = "0";
    public static final String DEFAULT_BEGIN     = "0";
    public static final String DEFAULT_MSG_COUNT = "100";


    private static CmdOpts opts = null;


    public static void main(String[] args) throws UnsupportedEncodingException, ParseException {
        opts = new CmdOpts(args, getCommandLineArguments(), Collections.emptySet());

        log.info("Starting Kafka Consumer...\n" + opts);

        fetchFromOffset(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)), Long.parseLong(opts.get(CLA_BEGIN)));

//
//        SimpleConsumer consumer = new SimpleConsumer("myd-vm23458.hpswlabs.adapps.hp.com", 9092, 100000, 64 * 1024, CLIENT_ID);
//        long offset = 0;
//
//        boolean readMore = true;
//        int index = 0;
//        while (readMore) {
//            FetchRequest chunk = getNextChunk(offset);
//            FetchResponse fetchResponse = consumer.fetch(chunk);
//            int messageCount = countMessages(fetchResponse);
//            long highWatermark = fetchResponse.highWatermark(TOPIC, PARTITION);
//            Utils.consolog("index=["+index+"] offset=["+offset+"] messageCount=["+ messageCount +"] hasError=["+fetchResponse.hasError()+"] highWatermark=["+ highWatermark +"] sizeInBytes=["+fetchResponse.messageSet(TOPIC, PARTITION).sizeInBytes()+"] validBytes=["+fetchResponse.messageSet(TOPIC, PARTITION).validBytes()+"] ");
//            index++;
//            offset += messageCount;
//            readMore = offset<highWatermark && index<MAX_ITERATIONS && !fetchResponse.hasError();
//        }
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
            log.info("Found ["+data.count()+"] messages from offset ["+offset+"]");
            log.info("assignment: " + Arrays.toString(consumer.assignment().toArray()));
//            for (ConsumerRecord<String, String> record : data.records(topicPartition)) {
//
//            }

        }
        catch (Exception e) {
            log.error("Failed in fetchFromOffset", e);
        }







    }

    private static void logConsumerMetrics(KafkaConsumer<String, String> consumer) {
        StringBuilder buf = new StringBuilder();
        buf.append("{ ");
        for (Metric metric : consumer.metrics().values()) {
            buf.append("\"").append(metric.metricName().name()).append("\" : \"").append(metric.value()).append("\", ");
        }
        buf.append(" }");
        log.info("Consumer Metrics: " + buf.toString());
    }

    private static Map<String, String> getCommandLineArguments() {
        Map<String, String> result = new HashMap<>();
        result.put(CLA_KAFKA, DEFAULT_KAFKA);
        result.put(CLA_TOPIC, DEFAULT_TOPIC);
        result.put(CLA_PARTITION, DEFAULT_PARTITION);
        result.put(CLA_BEGIN, DEFAULT_BEGIN);
//        result.put("kafka_host", "54.153.44.253");
//        result.put("kafka_port", "9092");
        return result;
    }

    private static void printMessageToConsole(FetchResponse fetchResponse) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(TOPIC, PARTITION)) {
            String payload = extractPayload(messageAndOffset);
            String key = extractKey(messageAndOffset);
            Utils.consolog("[K,V]=["+key+","+payload+"]");
        }
    }

    private static int countMessages(FetchResponse fetchResponse) {
        try {
            int count = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(TOPIC, PARTITION)) {
                count++;
            }
            return count;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private static FetchRequest getNextChunk(long offset) {
        return new FetchRequestBuilder().clientId(CLIENT_ID).addFetch(TOPIC, PARTITION, offset, CHUNK_SIZE).build();
    }

    private static String extractPayload(MessageAndOffset messageAndOffset) throws UnsupportedEncodingException {
        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        return new String(bytes, "UTF-8");
    }

    private static String extractKey(MessageAndOffset messageAndOffset) throws UnsupportedEncodingException {
        ByteBuffer key = messageAndOffset.message().key();
        byte[] bytes = new byte[key.limit()];
        key.get(bytes);
        return new String(bytes, "UTF-8");
    }

}
