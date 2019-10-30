package golan.izik.producer;

import golan.izik.mng.CmdOpts;
import golan.izik.mng.Utils;
import org.apache.commons.cli.ParseException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ProduceStringMessagesMultiThreads extends ProduceMessagesMultiThreads<String>{

    public ProduceStringMessagesMultiThreads(CmdOpts map) {
        super(map);
    }


    //TODO: replace the HashMap with CmdOpts

    public static void main(String[] args) throws ParseException, InterruptedException {
        CmdOpts cmdOpts = new CmdOpts(args, getParamsMap(), Collections.emptySet());
//        HashMap<String, String> cmdOpts = Utils.parseCommandLineArgs(args);
        System.out.println("====================================================================");
        System.out.printf("");
        ProduceStringMessagesMultiThreads runner = new ProduceStringMessagesMultiThreads(cmdOpts);
        runner.run();
    }

    private static Map<String, String> getParamsMap() {
        HashMap<String, String> result = new HashMap<>();
        result.put(Utils.ARG_SERVER, "localhost:9092"         );
        result.put(Utils.ARG_MESSAGE_PREFIX, ""               );
        result.put(Utils.ARG_TOPIC_PREFIX, "test"             );
        result.put(Utils.ARG_NUM_OF_TOPICS, "1"               );
        result.put(Utils.ARG_NUM_OF_PRODUCERS, "1"            );
        result.put(Utils.ARG_MESSAGE_PER_PRODUCER, "1"        );
        result.put(Utils.ARG_MAX_TIME_TO_WAIT, "25"           );
        result.put(Utils.ARG_KEY_GENERATOR, "UUIDKeyGenerator");
        return result;
    }

    @Override
    protected List<String> generateMessages(String topicName, String messagePrefix, int messagesCount) {

        long index = new Random().nextInt(10000)*1000;
        List<String> messages = new ArrayList<>(messagesCount);
        for (int m = 0; m < messagesCount; m++) {
            messages.add(messagePrefix + index++);
        }
        return messages;
    }

    @Override
    protected ProducerTask.KeyGenerator<String> getKeyGen() {
        if ("UUIDKeyGenerator".equals(cmdOpts.get(Utils.ARG_KEY_GENERATOR))) return new UUIDKeyGenerator();
        if ("ProducerIdKeyGen".equals(cmdOpts.get(Utils.ARG_KEY_GENERATOR))) return new ProducerIdKeyGen();
        if ("RoundRobinIntegerKeyGen".equals(cmdOpts.get(Utils.ARG_KEY_GENERATOR))) return new RoundRobinIntegerKeyGen();
        throw new IllegalArgumentException("Unknown KeyGen class. Unexpected value of ["+Utils.ARG_KEY_GENERATOR+"] command line argument");
    }

    protected static class UUIDKeyGenerator implements ProducerTask.KeyGenerator<String> {
        @Override
        public String generateKey(String message, String topicName, int producerId) {
            if (topicName !=null) topicName = topicName + "_";
            return topicName + UUID.randomUUID().toString();
        }
    }

    protected static class RoundRobinIntegerKeyGen implements ProducerTask.KeyGenerator<String> {
        private static int counter = 0;
        @Override
        public String generateKey(String message, String topicName, int producerId) {
            synchronized (RoundRobinIntegerKeyGen.class) {
                if (counter>Integer.MAX_VALUE/2) counter = 0;
                return String.valueOf(counter++);
            }
        }
    }

    protected static class ProducerIdKeyGen implements ProducerTask.KeyGenerator<String> {
        @Override
        public String generateKey(String message, String topicName, int producerId) {
            return String.valueOf(producerId);
        }
    }
}
