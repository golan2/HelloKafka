package golan.izik.producer;

import golan.izik.mng.Utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class RunInMainThread {

    private static final int SIZE = 300;
    private static final ProduceStringMessagesMultiThreads.UUIDKeyGenerator UUID_KEYGEN = new ProduceStringMessagesMultiThreads.UUIDKeyGenerator();

    public static void main(String[] args) throws java.text.ParseException {

        Utils.consolog(Utils.getCurrentTimeStamp() + " Run ProducerTask...");

        ArrayList<String> messages = new ArrayList<>(SIZE);


        final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS");
        final Date date = DF.parse("1963-02-01T00:00:00,000");
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        for (int i = 0; i < SIZE; i++) {
            cal.add(Calendar.MILLISECOND, 1);
            messages.add("{\"@timestamp\":\""+DF.format(cal.getTime())+"Z\",\"time\":\""+DF.format(cal.getTime())+"000000Z\",\"DataFlow project\":\"load-test-project-a21aa893\",\"DataFlow environment\":\"prod\",\"DataFlow organization\":\"load-test-org\",\"DataFlow component\":\"\",\"loglevel\":\"DEBUG\",\"Message\":\""+String.format("Document only in DEBUG %4d", i)+"\",\"host\":\"filebeat-5mgw5\",\"prospector\":{\"type\":\"log\"},\"datacenter\":{\"name\":\"dataflow-load-westus\"},\"source\":\"/hostfs/var/lib/docker/containers/b9feecd7ecb19dbc3032281b4d2c9147e04c46b71c5d936f9190170cbe0b86db/b9feecd7ecb19dbc3032281b4d2c9147e04c46b71c5d936f9190170cbe0b86db-json.log\",\"offset\":703583,\"unique originating id\":\"5d2d82e0-a472-4ec7-a791-ecf9220e9d63\",\"tags\":[\"beats_input_raw_event\"],\"identity of the caller\":\"middleware:_default\",\"stream\":\"stdout\",\"beat\":{\"name\":\"filebeat-5mgw5\",\"hostname\":\"filebeat-5mgw5\",\"version\":\"6.0.0\"},\"@version\":\"1\",\"customer facing\":\"N\"}");
        }

        ProducerTask<String> task = new ProducerTask<>(UUID_KEYGEN, "localhost:9092", messages, "data-in-test");
        task.run();
        Utils.consolog(Utils.getCurrentTimeStamp() + " Done!");
    }
}
