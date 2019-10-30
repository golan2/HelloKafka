package com.atnt.analytics.t16;

import config.MessageCountConf;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * For testing MsgCountHealth use these 2 commands:
 * [1] --objects 6 --messages 30 --start-time "2018-01-01 00:00"
 * [2] --objects 1 --messages 1 --start-time "2018-01-02 00:00"
 * In that order!
 *
 */
@Slf4j
public class MessageCountProducer implements Runnable {


    private static final DateTimeFormatter DF_TIMESTAMP  = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static final String ORG      = "org";
    private static final String PROJ     = "proj";
    private static final String ENV      = "env";
    private static final String ENV_UUID = "6f33d5e8-f410-11e8-b5e9-79dc71f7ba86";

    private final MessageCountConf kafkaConfig;
    private final String           objectId;
    private final String           runId;

    MessageCountProducer(String runId, MessageCountConf conf, String objectId) {
        this.runId = runId;
        this.kafkaConfig = conf;
        this.objectId = objectId;
    }

    @Override
    public void run() {
        int millisecondsBetweenMessages = 60*1000 / kafkaConfig.getMessagesPerMinute();
        try (Producer<String, String> producer = new KafkaProducer<>(getKafkaProperties())) {
            for (int m = 0; m< kafkaConfig.getMinutesOfData() ; m++) {
                final ZonedDateTime minute = kafkaConfig.getStartTime().plus(m, ChronoUnit.MINUTES);
                for (int i = 0; i < kafkaConfig.getMessagesPerMinute() ; i++) {
                    final String key = UUID.randomUUID().toString();
                    final String message = message(minute.plus(i*millisecondsBetweenMessages, ChronoUnit.MILLIS));

                    log.trace("[KAFKA_SEND] {} {}", key, message);
                    producer.send(new ProducerRecord<>(this.kafkaConfig.getTopic(), key, message), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception==null) {
                                log.trace("[KAFKA_PARTITION] {} {}", key, metadata.partition());
                            }
                            else {
                                log.error("[KAFKA_ERROR]", exception);
                            }
                        }
                    });
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafkaConfig.getBootstrapServers());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private String message(ZonedDateTime timestamp) {
        return message(UUID.randomUUID().toString(), timestamp.toInstant().toEpochMilli(), ORG, PROJ, ENV, ENV_UUID, objectId, timestamp);

    }

    @SuppressWarnings("SameParameterValue")
    private String message(String txnId, long timestamp, String organization, String project, String environment, String envUuid, String objectId, ZonedDateTime ts) {
        return String.format(messageTemplate_, txnId, timestamp, organization, project, organization, project, environment, envUuid, objectId, ts.format(DF_TIMESTAMP), runId);
    }

    private static final String messageTemplate_ = ("" +
            "{\n" +
            "\t\"ing_msg_tnxid\": \"%s\",\n" +                                              //TXN_ID
            "\t\"ing_msg_timestamp\": \"%d\",\n" +                                          //TIMESTAMP
            "\t\"ing_msg_org_bucket\": \"%s\",\n" +                                         //ORG_B
            "\t\"ing_msg_project_bucket\": \"%s\",\n" +                                     //PROJ_B
            "\t\"ing_msg_orgid\": \"%s\",\n" +                                              //ORG
            "\t\"ing_msg_projectid\": \"%s\",\n" +                                          //PROJ
            "\t\"ing_msg_environment\": \"%s\",\n" +                                        //ENV
            "\t\"ing_msg_envuuid\": \"%s\",\n" +                                            //ENV_UUID
            "\t\"ing_msg_device_type\": \"vehicle\",\n" +
            "\t\"ing_msg_deviceId\": \"%s\",\n" +                                           //OBJECT_ID
            "\t\"user_param\": {\n" +
            "\t\t\"humanTimestamp\": {\n" +
            "\t\t\t\"value\": \"%s\",\n" +                                                  //FORMATTED TIMESTAMP
            "\t\t\t\"name\": \"text\",\n" +
            "\t\t\t\"id\": 1\n" +
            "\t\t},\n" +
            "\t\t\"runId\": {\n" +
            "\t\t\t\"value\": \"%s\",\n" +                                                      //RUN ID
            "\t\t\t\"name\": \"number\",\n" +
            "\t\t\t\"id\": 1\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}")
            .replace("\n", "").replace("\t", "");

//    private static final String messageT = String.format(messageTemplate_, "%s", "%s", "Project1", "Project1", "Organization1", "Organization1", "Environment1");


//    private static Tuple2<String, String> generateMessage() {
//        int deviceIndex = ThreadLocalRandom.current().nextInt(0, devices.length-1);
//        int rawDataLength = ThreadLocalRandom.current().nextInt(50, 70);
//        String uuid = UUID.randomUUID().toString();
//        final String message = String.format(messageTemplate, System.currentTimeMillis(), devices[deviceIndex], uuid, generateRandHex(rawDataLength));
//        System.out.printf("~~generateMessage: TS=[%d] D=[%d] B=[%s]\nfullMessage = %s", System.currentTimeMillis(), deviceIndex, uuid, message);
//        return new Tuple2<>(uuid, message);
//    }
//
//    private static String generateRandHex(int length){
//        StringBuilder sb = new StringBuilder();
//        while(sb.length() < length){
//            sb.append(Integer.toHexString(ThreadLocalRandom.current().nextInt()));
//        }
//
//        return sb.toString().substring(0, length);
//    }

}
