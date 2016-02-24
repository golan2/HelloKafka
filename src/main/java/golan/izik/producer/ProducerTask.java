package golan.izik.producer;

import golan.izik.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by golaniz on 09/02/2016.
 */
public class ProducerTask<T> implements Runnable {
    private static int producer_index = 1;
    private final int producerId;
    private final String kafkaServer;
    private final List<T> messages;
    private final String topicName;
    private final KeyGenerator<T> keygen;
    private boolean finished = false;

    ProducerTask(KeyGenerator<T> keygen, String kafkaServer, List<T> messages, String topicName) {
        this.kafkaServer = kafkaServer;
        this.messages = messages;
        this.producerId = producer_index++;
        this.topicName = topicName;
        this.keygen = keygen;
    }

    @Override
    public void run() {
        try {
            Utils.consolog("ProducerTask - run - BEGIN - PID=[" + this.producerId + "] topicName=[" + topicName + "] messagesCount=[" + messages.size() + "]");
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            Producer<String, T> producer = new KafkaProducer<>(props);

            Utils.consolog("PID=[" + this.producerId + "] Sending [" + messages.size() + "] messages to topic [" + topicName + "]...");
            for (T message : messages) {
                String key = this.keygen.generateKey(message, this.topicName, this.producerId);
                Utils.consolog("\ttopicName=[" + topicName + "] key=[" + key + "] message=[" + message + "]...");
                producer.send(new ProducerRecord<>(topicName, key, message));
            }
            Utils.consolog("PID=[" + this.producerId + "] Done Sending!");

            Utils.consolog("PID=[" + this.producerId + "] Closing...");
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
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

    public interface KeyGenerator<T> {
        String generateKey(T message, String topicName, int producerId);
    }


}
