package golan.izik.producer;

import golan.izik.Utils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by golaniz on 09/02/2016.
 */
public class ProducerTask implements Runnable {
    private static int producer_index = 1;
    private final int producerId;
    private final String kafkaServer;
    private final List<String> messages;
    private final String topicName;
    private boolean finished = false;

    ProducerTask(String kafkaServer, List<String> messages, String topicName) {
        this.kafkaServer = kafkaServer;
        this.messages = messages;
        this.producerId = producer_index++;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        try {
            Utils.consolog("ProducerTask - run - BEGIN - PID=["+this.producerId+"] topicName=["+topicName+"] messagesCount=["+messages.size()+"]");
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            Producer<String, String> producer = new KafkaProducer<>(props);

            Utils.consolog("PID=["+this.producerId+"] Sending ["+messages.size()+"] messages to topic ["+topicName+"]...");
            int index = 1;
            for (String message : messages) {
                String msg = this.producerId + "_" + message;
//                String msg = "Msg=[" + message + "] MsgIndx=[" + index + "/" + messages.size() + "] PID=[" + this.producerId + "]";
                UUID key = UUID.randomUUID();
                Utils.consolog("Sending key=["+key+"] msg=["+msg+"]...");
                producer.send(new ProducerRecord<>(topicName, key.toString(), msg));
                index++;
            }
            Utils.consolog("PID=["+this.producerId+"] Done Sending!");


            Utils.consolog("PID=["+this.producerId+"] Closing...");
            producer.close();
        } finally {
            finished = true;
        }
        Utils.consolog("ProducerTask - run - END - PID=["+this.producerId+"]");
    }

    public int getProducerId() {
        return producerId;
    }

    public boolean isFinished() {
        return finished;
    }

    private static class DummyProd implements Producer<String,String> {

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
            return null;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> producerRecord, Callback callback) {
            return null;
        }


        @Override
        public List<PartitionInfo> partitionsFor(String s) {
            return null;
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return null;
        }

        @Override
        public void close() {

        }

    }
}
