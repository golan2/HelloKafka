package golan.izik.producer;

import golan.izik.mng.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;

/**
 * Created by golaniz on 09/02/2016.
 */
public class ProducerTask<T> implements Runnable {
    public static final boolean DEBUG = false;
    private static int producer_index = 1;
    private final int producerId;
    private final String kafkaServer;
    private final List<T> messages;
    private final String topicName;
    private final KeyGenerator<T> keygen;
    private CC callbackHandler = null;
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
            if (DEBUG) Utils.consolog("ProducerTask - run - BEGIN - PID=[" + this.producerId + "] topicName=[" + topicName + "] messagesCount=[" + messages.size() + "]");
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            this.callbackHandler = new CC();
            Producer<String, T> producer = new KafkaProducer<>(props);

            long startTimeNano = System.nanoTime();
            if (DEBUG) Utils.consolog("PID=[" + this.producerId + "] Sending [" + messages.size() + "] messages to topic [" + topicName + "]...");
            StringBuilder buf = new StringBuilder();
            for (T message : messages) {
                String key = this.keygen.generateKey(message, this.topicName, this.producerId);
                if (DEBUG) Utils.consolog("\t[SEND] topicName=[" + topicName + "] key=[" + key + "] message=[" + message + "]...");
                buf.append("["+key+","+message+"] ; ");
                producer.send(new ProducerRecord<>(topicName, key, message), callbackHandler);
            }
            Utils.consolog("PID=[" + this.producerId + "] Done Sending ["+messages.size()+"] messages: "+buf.toString());

            if (DEBUG) Utils.consolog("PID=[" + this.producerId + "] Closing...");
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

    public long getDeltaTime() {
        return callbackHandler.lastTimeNano - callbackHandler.beginTimeNano;
    }

    public interface KeyGenerator<T> {
        String generateKey(T message, String topicName, int producerId);
    }


    private class CC implements Callback {
        final long beginTimeNano = System.nanoTime();
        long lastTimeNano = 0;
        @Override
        public void onCompletion(RecordMetadata r, Exception e) {
            long nano = System.nanoTime();
            if (DEBUG) Utils.consolog("\t[ACK] partition=[" + r.partition() + "] offset=[" + r.offset() + "] nano=[" + nano + "]");
            if(nano>lastTimeNano) lastTimeNano = nano;
        }


    }
}
