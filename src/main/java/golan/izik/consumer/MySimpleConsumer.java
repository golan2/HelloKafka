package golan.izik.consumer;

import golan.izik.Utils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by golaniz on 12/02/2016.
 */
public class MySimpleConsumer {

    public static final String CLIENT_ID = "ClientId_Izik";
    public static final String TOPIC = "test1";
    public static final int PARTITION = 0;
    public static final int CHUNK_SIZE = 10000;
    private static final int MAX_ITERATIONS = 100;


    public static void main(String[] args) throws UnsupportedEncodingException {
        SimpleConsumer consumer = new SimpleConsumer("myd-vm23458.hpswlabs.adapps.hp.com", 9092, 100000, 64 * 1024, CLIENT_ID);
        long offset = 0;

        boolean readMore = true;
        int index = 0;
        while (readMore) {
            FetchRequest chunk = getNextChunk(offset);
            FetchResponse fetchResponse = consumer.fetch(chunk);
            int messageCount = countMessages(fetchResponse);
            long highWatermark = fetchResponse.highWatermark(TOPIC, PARTITION);
            Utils.consolog("index=["+index+"] offset=["+offset+"] messageCount=["+ messageCount +"] hasError=["+fetchResponse.hasError()+"] highWatermark=["+ highWatermark +"] sizeInBytes=["+fetchResponse.messageSet(TOPIC, PARTITION).sizeInBytes()+"] validBytes=["+fetchResponse.messageSet(TOPIC, PARTITION).validBytes()+"] ");
            index++;
            offset += messageCount;
            readMore = offset<highWatermark && index<MAX_ITERATIONS && !fetchResponse.hasError();
        }
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
