package com.atnt.analytics.t16;

import config.MessageCountConf;
import org.apache.commons.cli.ParseException;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Produce Kafka messages in a desired message rate (event time rate not ingestion time rate).
 * It is not about rate of writing to Kafka rather than having in Kafka messages with event time that simulates as-if they were ingested in the desired rate.
 * It is meant for checking later the messages and run calculations that base on message rate (like CounterBasedHealth)
 *
 * @see MessageCountConf
 */
public class MessageCountMain {

    private static final String RUN_ID = UUID.randomUUID().toString();
    private static final int THREAD_POOL_SIZE = 1;


    public static void main(String[] args) throws ParseException, InterruptedException {
        final MessageCountConf conf = new MessageCountConf(args);
        System.out.println("=================================");
        System.out.println("RUN_ID: " + RUN_ID);
        System.out.println("Configuration: " + conf);
        System.out.println("=================================");
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        MessageCountProducer[] producerPerObject = new MessageCountProducer[conf.getObjects()];

        for (int i = 0; i < producerPerObject.length; i++) {
            producerPerObject[i] = new MessageCountProducer(RUN_ID, conf, String.format("obj_%03d", i));
            executorService.submit(producerPerObject[i]);
            System.out.println("~~Producer for object ["+i+"] was submitted. ("+producerPerObject[i].hashCode()+")");
        }
        System.out.println("~~Waiting...");
        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);
        executorService.shutdownNow();

        System.out.println("~~DONE!");
    }

}
