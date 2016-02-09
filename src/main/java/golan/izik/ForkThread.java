package golan.izik;

import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
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
        ArrayList<ProducerTask> producers = new ArrayList<>(producersCount);


        for (int t = 0; t < topicsCount; t++) {
            String topicName = topicPrefix + String.valueOf(t+1);
            for (int p = 0; p < producersCount; p++) {
                List<String> messages = generateMessages(messagePrefix, messagesCount);
                ProducerTask task = new ProducerTask(map.get(Utils.ARG_SERVER), messages, topicName);
                producers.add(task);
                System.out.println(Utils.getCurrentTimeStamp() + " Submitting ProducerTask - PID=["+task.getProducerId()+"] Topic=["+topicName+"]");
                executorService.submit(task);
            }
        }

        int count = 0;
        int maxTimeToWait  = Integer.parseInt(map.get(Utils.ARG_MAX_TIME_TO_WAIT));
        int maxRetry = maxTimeToWait*1000/SLEEP_TIME ;
        while (!allFinished(producers) && count< maxRetry){
            count++;
            System.out.println(Utils.getCurrentTimeStamp() + " Waiting ["+count+"]...");
            Thread.sleep(SLEEP_TIME);
        }


        System.out.println(Utils.getCurrentTimeStamp() + " Shutting down the executor service...");
        executorService.shutdown();
        System.out.println(Utils.getCurrentTimeStamp() + " Done!");


    }

    private static boolean allFinished(ArrayList<ProducerTask> producers) {
        for (ProducerTask producer : producers) {
            if (!producer.isFinished()) return false;
        }
        return true;
    }

    private static List<String> generateMessages(String prefix, int messagesCount) {
        Random random = new Random();
        List<String> messages = new ArrayList<String>(messagesCount);
        for (int m = 0; m < messagesCount; m++) {
            messages.add(prefix + "{"+random.nextInt(100000)+"}");
        }
        return messages;
    }


}
