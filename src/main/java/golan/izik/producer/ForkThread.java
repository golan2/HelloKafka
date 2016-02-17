package golan.izik.producer;

import golan.izik.Utils;
import org.apache.commons.cli.ParseException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ForkThread {

    public static final int SLEEP_TIME = 5000;

    public static void main(String[] args) throws InterruptedException, ParseException {

        HashMap<String, String> map = Utils.parseCommandLineArgs(args);

        String topicPrefix = map.get(Utils.ARG_TOPIC_PREFIX);
        String messagePrefix = map.get(Utils.ARG_MESSAGE_PREFIX);
        int producersCount = Integer.parseInt(map.get(Utils.ARG_NUM_OF_PRODUCERS));
        int topicsCount = Integer.parseInt(map.get(Utils.ARG_NUM_OF_TOPICS));
        int messagesCount = Integer.parseInt(map.get(Utils.ARG_MESSAGE_PER_PRODUCER));

        ExecutorService executorService = Executors.newFixedThreadPool(producersCount);
        Map<String, List<ProducerTask>> producers = new HashMap<>();
//        ArrayList<ProducerTask> producers = new ArrayList<>(producersCount);


        for (int t = 0; t < topicsCount; t++) {
            String topicName = topicPrefix + String.valueOf(t+1);
            ArrayList<ProducerTask> tasks = new ArrayList<>(producersCount);
            for (int p = 0; p < producersCount; p++) {
                List<String> messages = generateMessages(messagePrefix, messagesCount);
                ProducerTask task = new ProducerTask(map.get(Utils.ARG_SERVER), messages, topicName);
                tasks.add(task);
            }
            producers.put(topicName, tasks);
        }

        for (String topicName : producers.keySet()) {
            List<ProducerTask> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                Utils.consolog("Submitting ProducerTask - PID=["+task.getProducerId()+"] Topic=["+topicName+"]");
                executorService.submit(task);
            }
        }

        int count = 0;
        int maxTimeToWait  = Integer.parseInt(map.get(Utils.ARG_MAX_TIME_TO_WAIT));
        int maxRetry = maxTimeToWait*1000/SLEEP_TIME ;
        while (!allFinished(producers) && count< maxRetry){
            count++;
            Utils.consolog("Waiting ["+count+"]...");
            Thread.sleep(SLEEP_TIME);
        }


        Utils.consolog("Shutting down the executor service...");
        executorService.shutdown();
        Utils.consolog("Done!");


    }

    private static boolean allFinished(Map<String, List<ProducerTask>> producers) {
        for (String topicName : producers.keySet()) {
            List<ProducerTask> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                if (!task.isFinished()) return false;
            }
        }
        return true;
    }

    private static List<String> generateMessages(String prefix, int messagesCount) {

        long index = new Random().nextInt(10000)*1000;
        List<String> messages = new ArrayList<String>(messagesCount);
        for (int m = 0; m < messagesCount; m++) {
            messages.add(prefix + index++);
        }
        return messages;
    }


}
