package golan.izik.consumer;

import golan.izik.mng.CmdOpts;
import golan.izik.mng.Utils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LowLevelConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithOffset.class);

    public static final String CLIENT_ID = "ClientId_Izik";

    private static final String CLA_KAFKA_HOST = "kafka_host";
    private static final String CLA_KAFKA_PORT = "kafka_port";
    private static final String CLA_TOPIC      = "topic";
    private static final String CLA_PARTITION  = "partition";

    private static final String DEFAULT_KAFKA_HOST = "localhost";
    private static final String DEFAULT_KAFKA_PORT = "9092";
    private static final String DEFAULT_TOPIC      = "test";
    private static final String DEFAULT_PARTITION  = "0";



    public static final int CHUNK_SIZE = 10000;
    private static final int MAX_ITERATIONS = 15;

    private static CmdOpts opts = null;

    public static void main(String[] args) throws ParseException {
        opts = new CmdOpts(args, getCommandLineArguments(), Collections.emptySet());

        SimpleConsumer consumer = new SimpleConsumer("myd-vm23458.hpswlabs.adapps.hp.com", 9092, 100000, 64 * 1024, CLIENT_ID);
        long offset = 0;

        boolean readMore = true;
        int index = 0;
        while (readMore) {
            FetchRequest chunk = getNextChunk(offset);
            FetchResponse fetchResponse = consumer.fetch(chunk);
            int messageCount = countMessages(fetchResponse);
            long highWatermark = fetchResponse.highWatermark(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)));
            Utils.consolog("index=["+index+"] offset=["+offset+"] messageCount=["+ messageCount +"] hasError=["+fetchResponse.hasError()+"] highWatermark=["+ highWatermark +"] sizeInBytes=["+fetchResponse.messageSet(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION))).sizeInBytes()+"] validBytes=["+fetchResponse.messageSet(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION))).validBytes()+"] ");
            index++;
            offset += messageCount;
            readMore = offset<highWatermark && index<MAX_ITERATIONS && !fetchResponse.hasError();
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

    private static void printMessageToConsole(FetchResponse fetchResponse) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)))) {
            String payload = extractPayload(messageAndOffset);
            String key = extractKey(messageAndOffset);
            Utils.consolog("[K,V]=["+key+","+payload+"]");
        }
    }

    private static int countMessages(FetchResponse fetchResponse) {
        try {
            int count = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)))) {
                count++;
            }
            return count;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private static FetchRequest getNextChunk(long offset) {
        return new FetchRequestBuilder().clientId(CLIENT_ID).addFetch(opts.get(CLA_TOPIC), Integer.parseInt(opts.get(CLA_PARTITION)), offset, CHUNK_SIZE).build();
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


    private static Map<String, String> getCommandLineArguments() {
        Map<String, String> result = new HashMap<>();
        result.put(CLA_KAFKA_HOST, DEFAULT_KAFKA_HOST);
        result.put(CLA_KAFKA_PORT, DEFAULT_KAFKA_PORT);
        result.put(CLA_TOPIC, DEFAULT_TOPIC);
        result.put(CLA_PARTITION, DEFAULT_PARTITION);
        return result;
    }
}
