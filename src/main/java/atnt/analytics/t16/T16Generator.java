package atnt.analytics.t16;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class T16Generator {

    private static final int SIZE = 1;
    private static final int MESSAGE_COUNT = 1;


    public static void main(String[] args) throws InterruptedException {
//       T16Producer.generateMessage();
        new T16Producer(1, 5000).run();

    }

    private static void original(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(SIZE);
        T16Producer[] producers = new T16Producer[SIZE];
        int messageCount = (args.length>0) ? Integer.parseInt(args[0]) : MESSAGE_COUNT;
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new T16Producer(messageCount, 5000);
            executorService.submit(producers[i]);
            System.out.println("~~Producer submitted: ["+producers[i].hashCode()+"]");
        }
        System.out.println("~~Waiting...");
        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);
        executorService.shutdownNow();
        System.out.println("~~DONE!");
    }

}
