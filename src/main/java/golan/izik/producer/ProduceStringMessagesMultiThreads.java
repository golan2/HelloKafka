package golan.izik.producer;

import golan.izik.Utils;
import org.apache.commons.cli.ParseException;

import java.util.*;

public class ProduceStringMessagesMultiThreads extends ProduceMessagesMultiThreads<String>{

    public ProduceStringMessagesMultiThreads(HashMap<String, String> map) {
        super(map);
    }


    //TODO: replace the HashMap with CmdOpts
    //TODO: Generify by extracting <String> out

    public static void main(String[] args) throws ParseException, InterruptedException {
        HashMap<String, String> map = Utils.parseCommandLineArgs(args);
        ProduceStringMessagesMultiThreads runner = new ProduceStringMessagesMultiThreads(map);
        runner.run();
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
    protected ProducerTask.KeyGenerator getKeyGen() {
        return new UUIDKeyGenerator();
    }

    protected static class UUIDKeyGenerator implements ProducerTask.KeyGenerator<String> {

        @Override
        public String generateKey(String message, String topicName, int producerId) {
            if (topicName !=null) topicName = topicName + "_";
            return topicName + UUID.randomUUID().toString();
        }
    }
}
