package com.atnt.analytics.t16;

import config.MyKafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The format looks like {@link #messageTemplate}
 *
 */
public class T16Producer implements Runnable {

    private static final String[] devices       = {"D1", "D2", "D3", "D4"};

    private final MyKafkaConfig kafkaConfig;
    private final int messageCount;
    private final int messageTPS;       //txn per sec

    T16Producer(MyKafkaConfig kafkaConfig, int messageCount, int messageTPS) {
        this.kafkaConfig = kafkaConfig;
        this.messageCount = messageCount;
        this.messageTPS = messageTPS;
    }

    @Override
    public void run() {
        try (Producer<String, String> producer = new KafkaProducer<>(getKafkaProperties())) {
            for (int i = 0; i < messageCount; i++) {
                Tuple2<String, String> msg = generateMessage();
                producer.send(new ProducerRecord<>(this.kafkaConfig.getTopic(), msg._1(), msg._2()));
                Thread.sleep(Math.round(1000.0/messageTPS));
                System.out.println("~~Producer ["+hashCode()+"] sent message ["+msg._1()+"]");
            }

        } catch (InterruptedException e) {
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

    private static Tuple2<String, String> generateMessage() {
        int deviceIndex = ThreadLocalRandom.current().nextInt(0, devices.length-1);
        int rawDataLength = ThreadLocalRandom.current().nextInt(50, 70);
        String uuid = UUID.randomUUID().toString();
        final String message = String.format(messageTemplate, System.currentTimeMillis(), devices[deviceIndex], uuid, generateRandHex(rawDataLength));
        System.out.printf("~~generateMessage: TS=[%d] D=[%d] B=[%s]\nfullMessage = %s", System.currentTimeMillis(), deviceIndex, uuid, message);
        return new Tuple2<>(uuid, message);
    }

    private static String generateRandHex(int length){
        StringBuilder sb = new StringBuilder();
        while(sb.length() < length){
            sb.append(Integer.toHexString(ThreadLocalRandom.current().nextInt()));
        }

        return sb.toString().substring(0, length);
    }

    private final static String messageTemplate = ("" +
            "{\n" +
            "  \"q1w2e3_timestamp\": \"%d\",\n" +                               //TIMESTAMP
            "  \"q1w2e3_environment\": \"n\",\n" +
            "  \"q1w2e3_userid\": \"USER\",\n" +
            "  \"q1w2e3_projectid\": \"PROJ\",\n" +
            "  \"q1w2e3_deviceId\": \"%s\",\n" +                                //DEVICE ID
            "  \"q1w2e3_device_type\": \"5000\",\n" +
            "  \"q1w2e3_device_firmware\": \"18g-160\",\n" +
            "  \"q1w2e3_user_bucket\": \"%s\",\n" +                             //USER BUCKET
            "  \"q1w2e3_project_bucket\": \"P-BUCKET\",\n" +
            "  \"user_param\": {\n" +
            "    \"values\": {\n" +
            "      \"input1\": \"true\",\n" +
            "      \"input2\": \"true\",\n" +
            "      \"rawData\": \"%s\",\n" +                                    //RAW_DATA
            "      \"gpsInvalidFix\": \"false\",\n" +
            "      \"historic\": \"true\",\n" +
            "      \"gpsReceiverSelfTestError\": \"false\",\n" +
            "      \"updateTime\": \"2018-01-08T21:33:39.000Z\",\n" +
            "      \"mobileIdTypeEnabled\": \"false\",\n" +
            "      \"available\": \"true\",\n" +
            "      \"deviceType\": \"5000\",\n" +
            "      \"forwardingEnabled\": \"false\",\n" +
            "      \"gpsReceiverTrackingError\": \"false\",\n" +
            "      \"derivedMileage\": 8113.428,\n" +
            "      \"m2xDeviceID\": \"213465c6623ec90802a0923b04a54a3c\",\n" +
            "      \"firmware\": \"18g-160\",\n" +
            "      \"vinSource\": \"file\",\n" +
            "      \"UMTS\": \"false\",\n" +
            "      \"spare\": 0,\n" +
            "      \"assetIdentifier\": \"\",\n" +
            "      \"satellites\": 12,\n" +
            "      \"input5\": \"true\",\n" +
            "      \"alwaysEnabled\": \"true\",\n" +
            "      \"lostExternalPower\": \"false\",\n" +
            "      \"heading\": 307,\n" +
            "      \"responseRedirectionEnabled\": \"false\",\n" +
            "      \"voiceCallIsActive\": \"false\",\n" +
            "      \"mobileIdEnabled\": \"true\",\n" +
            "      \"lgpDate\": \"2018-01-08T21:33:39.000Z\",\n" +
            "      \"userMsgRoute\": \"0\",\n" +
            "      \"lastKnown\": \"false\",\n" +
            "      \"timeofFix\": \"2018-01-08T21:33:39.000Z\",\n" +
            "      \"@location\": {\n" +
            "        \"longitude\": -111.1039893,\n" +
            "        \"latitude\": 45.7369836,\n" +
            "        \"elevation\": null,\n" +
            "        \"_type\": \"geo_point\"\n" +
            "      },\n" +
            "      \"input3\": \"true\",\n" +
            "      \"serviceType\": 1,\n" +
            "      \"locationFlag\": \"A\",\n" +
            "      \"optionsExtensionEnabled\": \"true\",\n" +
            "      \"routingEnabled\": \"false\",\n" +
            "      \"input6\": \"true\",\n" +
            "      \"deviceInputIgnition\": \"true\",\n" +
            "      \"carrier\": 410,\n" +
            "      \"messageType\": 4,\n" +
            "      \"processedAt\": \"2018-01-09T15:06:18.811Z\",\n" +
            "      \"firmwareScriptVersion\": 50.5,\n" +
            "      \"twoDimFix\": \"false\",\n" +
            "      \"input4\": \"true\",\n" +
            "      \"networkService\": \"true\",\n" +
            "      \"messageSequenceNumber\": 12413,\n" +
            "      \"malfunctionIndicatorLampStatusIndicator\": \"ON\",\n" +
            "      \"gpsAntennaStatusError\": \"false\",\n" +
            "      \"mobileIDType\": 1,\n" +
            "      \"HDOP\": 0.8,\n" +
            "      \"userMsgLen\": \"8\",\n" +
            "      \"lmu32HttpOtaUpdateStatusError\": \"false\",\n" +
            "      \"connected\": \"true\",\n" +
            "      \"dataService\": \"true\",\n" +
            "      \"speed\": 69.03,\n" +
            "      \"predicted\": \"false\",\n" +
            "      \"vtsEvent\": 99,\n" +
            "      \"clientId\": \"IOT\",\n" +
            "      \"altitude\": 138061,\n" +
            "      \"invalidTime\": \"false\",\n" +
            "      \"input7\": \"true\",\n" +
            "      \"mobileID\": \"5061005875\",\n" +
            "      \"MESSAGE_IN_BYTES\": 122,\n" +
            "      \"ICCID\": \"89014104255022865810\",\n" +
            "      \"rollspeed\": 69.03,\n" +
            "      \"userMsgID\": \"2\",\n" +
            "      \"RSSI\": -68,\n" +
            "      \"authenticationWordEnabled\": \"false\",\n" +
            "      \"phoneNumber\": \"18283016733\",\n" +
            "      \"differntiallyCorrected\": \"true\",\n" +
            "      \"timeStamp\": \"2018-01-08T21:33:39.000Z\",\n" +
            "      \"userMsg\": \"1,1,c0,3,9,0,0,ce\",\n" +
            "      \"roaming\": \"false\"\n" +
            "    },\n" +
            "    \"timestamp\": \"2018-01-09T15:06:18.461Z\",\n" +
            "    \"metadata\": {\n" +
            "      \"M2X-DEVICE-ID\": \"213465c6623ec90802a0923b04a54a3c\"\n" +
            "    },\n" +
            "    \"id\": \"075BCB2D29F120C1E2A449AEA50724FC\"\n" +
            "  }\n" +
            "};").replace("\n", "\t\t");

}
