package golan.izik.producer;

import golan.izik.Utils;
import org.apache.commons.cli.ParseException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProduceStringsMultiThreads {

    public static final int SLEEP_TIME = 5000;

    //TODO: replace the HashMap with CmdOpts

    public static void main(String[] args) throws InterruptedException, ParseException {

        HashMap<String, String> map = Utils.parseCommandLineArgs(args);

        int producersPerTopic = Integer.parseInt(map.get(Utils.ARG_NUM_OF_PRODUCERS));
        ExecutorService executorService = Executors.newFixedThreadPool(producersPerTopic);

        Map<String, List<ProducerTask<String>>> producers = createProducers(map);

        submitProducers(executorService, producers);

        awaitTermination(map, producers);

        Utils.consolog("Shutting down the executor service...");
        executorService.shutdown();
        Utils.consolog("Done!");


    }

    private static void awaitTermination(HashMap<String, String> map, Map<String, List<ProducerTask<String>>> producers) throws InterruptedException {
        int count = 0;
        int maxTimeToWait  = Integer.parseInt(map.get(Utils.ARG_MAX_TIME_TO_WAIT));
        int maxRetry = maxTimeToWait*1000/SLEEP_TIME ;
        while (!allFinished(producers) && count< maxRetry){
            count++;
            Utils.consolog("Waiting ["+count+"]...");
            Thread.sleep(SLEEP_TIME);
        }
    }

    private static void submitProducers(ExecutorService executorService, Map<String, List<ProducerTask<String>>> producers) {
        for (String topicName : producers.keySet()) {
            List<ProducerTask<String>> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                Utils.consolog("Submitting ProducerTask - PID=["+task.getProducerId()+"] Topic=["+topicName+"]");
                executorService.submit(task);
            }
        }
    }

    private static Map<String, List<ProducerTask<String>>> createProducers(HashMap<String, String> map) {
        int producersPerTopic = Integer.parseInt(map.get(Utils.ARG_NUM_OF_PRODUCERS));
        String topicPrefix = map.get(Utils.ARG_TOPIC_PREFIX);
        String messagePrefix = map.get(Utils.ARG_MESSAGE_PREFIX);
        int topicsCount = Integer.parseInt(map.get(Utils.ARG_NUM_OF_TOPICS));
        int messagesPerProducer = Integer.parseInt(map.get(Utils.ARG_MESSAGE_PER_PRODUCER));

        Map<String, List<ProducerTask<String>>> producers = new HashMap<>();        //map key is topic name
        for (int t = 0; t < topicsCount; t++) {
            String topicName = topicPrefix + String.valueOf(t+1);
            ArrayList<ProducerTask<String>> tasks = new ArrayList<>(producersPerTopic);
            for (int p = 0; p < producersPerTopic; p++) {
                List<String> messages = generateMessages(messagePrefix, messagesPerProducer);
                ProducerTask<String> task = new ProducerTask<>(map.get(Utils.ARG_SERVER), messages, topicName);
                tasks.add(task);
            }
            producers.put(topicName, tasks);
        }
        return producers;
    }

    private static boolean allFinished(Map<String, List<ProducerTask<String>>> producers) {
        for (String topicName : producers.keySet()) {
            List<ProducerTask<String>> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                if (!task.isFinished()) return false;
            }
        }
        return true;
    }

    private static List<String> generateMessages(String prefix, int messagesCount) {

        long index = new Random().nextInt(10000)*1000;
        List<String> messages = new ArrayList<>(messagesCount);
        for (int m = 0; m < messagesCount; m++) {
            messages.add(prefix + index++);
        }
        return messages;
    }


}
