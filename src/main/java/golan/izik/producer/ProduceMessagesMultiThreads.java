package golan.izik.producer;

import golan.izik.mng.CmdOpts;
import golan.izik.mng.Utils;

import javax.rmi.CORBA.Util;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by golaniz on 22/02/2016.
 */
public abstract class ProduceMessagesMultiThreads<T> {
    public static final int SLEEP_TIME = 5000;
    protected final CmdOpts cmdOpts;
    protected ExecutorService executorService;
    protected Map<String, List<ProducerTask<T>>> producers;     //key is topicName

    protected ProduceMessagesMultiThreads(CmdOpts cmdOpts) {
        this.cmdOpts = cmdOpts;
    }

    public void run() throws InterruptedException {
        int producersPerTopic = 5*Integer.parseInt(cmdOpts.get(Utils.ARG_NUM_OF_PRODUCERS));
        executorService = Executors.newFixedThreadPool(producersPerTopic);

        Utils.consolog("createProducers...");
        producers = createProducers();
        Utils.consolog("submitProducers...");
        submitProducers(executorService, producers);
        Utils.consolog("awaitTermination...");
        awaitTermination();
        Utils.consolog("Shutting down the executor service...");
        executorService.shutdown();
        Utils.consolog("Done!");

    }

    protected Map<String, List<ProducerTask<T>>> createProducers() {
        int producersPerTopic = Integer.parseInt(cmdOpts.get(Utils.ARG_NUM_OF_PRODUCERS));
        String topicPrefix = cmdOpts.get(Utils.ARG_TOPIC_PREFIX);
        String messagePrefix = cmdOpts.get(Utils.ARG_MESSAGE_PREFIX);
        int topicsCount = Integer.parseInt(cmdOpts.get(Utils.ARG_NUM_OF_TOPICS));
        int messagesPerProducer = Integer.parseInt(cmdOpts.get(Utils.ARG_MESSAGE_PER_PRODUCER));
        String kafkaServer = cmdOpts.get(Utils.ARG_SERVER);
        ProducerTask.KeyGenerator keyGen = getKeyGen();

        Map<String, List<ProducerTask<T>>> producers = new HashMap<>();        //cmdOpts key is topic name
        for (int t = 0; t < topicsCount; t++) {

            String topicName = topicPrefix + ((topicsCount>1) ? String.valueOf(t+1) : "");
            ArrayList<ProducerTask<T>> tasks = new ArrayList<>(producersPerTopic);
            for (int p = 0; p < producersPerTopic; p++) {
                List<T> messages = generateMessages(topicName,  messagePrefix, messagesPerProducer);
                ProducerTask<T> task = new ProducerTask<>(keyGen, kafkaServer, messages, topicName);
                tasks.add(task);
            }
            producers.put(topicName, tasks);
        }
        return producers;
    }

    protected abstract ProducerTask.KeyGenerator<T> getKeyGen();

    protected abstract List<T> generateMessages(String topicName, String messagePrefix, int messagesCount);

    protected void submitProducers(ExecutorService executorService, Map<String, List<ProducerTask<T>>> producers) {
        for (String topicName : producers.keySet()) {
            List<ProducerTask<T>> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                Utils.consolog("Submitting ProducerTask - PID=["+task.getProducerId()+"] Topic=["+topicName+"]");
                executorService.submit(task);
            }
        }
    }

    protected void awaitTermination() throws InterruptedException {
        int count = 0;
        int maxTimeToWait  = Integer.parseInt(cmdOpts.get(Utils.ARG_MAX_TIME_TO_WAIT));
        int maxRetry = maxTimeToWait*1000/SLEEP_TIME ;
        Utils.consolog("maxTimeToWait=["+maxTimeToWait+"] maxRetry=["+maxRetry+"] ");
        while (!allFinished() && count< maxRetry){
            count++;
            Utils.consolog("Waiting ["+count+"]...");
            Thread.sleep(SLEEP_TIME);
        }
        StringBuilder buf = new StringBuilder();
        for (String topic : producers.keySet()) {
            buf.append("\tTopic=["+topic+"]: ");
            List<ProducerTask<T>> producers = this.producers.get(topic);
            for (ProducerTask<T> producer : producers) {
                buf.append("("+producer.getProducerId()+","+producer.getDeltaTime()/1000000000.0+") ; ");
            }
            buf.append("\n");
        }
        Utils.consolog("Done waiting. count=["+count+"] allFinished=["+allFinished()+"]");
        Utils.consolog("Deltas:\n"+buf.toString());
    }

    private boolean allFinished() {
        for (String topicName : producers.keySet()) {
            List<ProducerTask<T>> tasks = producers.get(topicName);
            for (ProducerTask task : tasks) {
                if (!task.isFinished()) return false;
            }
        }
        return true;
    }
}
