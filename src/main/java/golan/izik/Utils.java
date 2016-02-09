package golan.izik;

import org.apache.commons.cli.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by golaniz on 09/02/2016.
 */
public class Utils {
    public static final String ARG_SERVER               = "server";
    public static final String ARG_MESSAGE_PREFIX       = "message_prefix";
    public static final String ARG_TOPIC_PREFIX         = "topic_prefix";
    public static final String ARG_NUM_OF_TOPICS        = "topics";
    public static final String ARG_NUM_OF_PRODUCERS     = "producers";
    public static final String ARG_MESSAGE_PER_PRODUCER = "messages";
    public static final String ARG_MAX_TIME_TO_WAIT     = "max_ttw";

    public static String getCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

    static HashMap<String, String> parseCommandLineArgs(String[] args) throws ParseException {
        Options o = new Options();
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Kafka Server").withDescription("Where is Kafka").isRequired(false).create(ARG_SERVER));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Topic Prefix").withDescription("The Prefix for each message").isRequired(false).create(ARG_TOPIC_PREFIX));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Topics Count").withDescription("How may topics").isRequired(false).create(ARG_NUM_OF_TOPICS));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Producers per Topic").withDescription("How may producer threads per topic").isRequired(false).create(ARG_NUM_OF_PRODUCERS));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Messages per Producer").withDescription("How may messages each producer will generate").isRequired(false).create(ARG_MESSAGE_PER_PRODUCER));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Message Prefix").withDescription("The Prefix for each message").isRequired(false).create(ARG_MESSAGE_PREFIX));
        o.addOption(OptionBuilder.hasArgs(1).withArgName("Max Time to Wait").withDescription("How long (seconds) will the main thread wait for the producers to end").isRequired(false).create(ARG_MAX_TIME_TO_WAIT));
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        HashMap<String, String> result = new HashMap<>();
        result.put(ARG_SERVER, line.getOptionValue              (ARG_SERVER, "localhost:9092" ));
        result.put(ARG_MESSAGE_PREFIX, line.getOptionValue      (ARG_MESSAGE_PREFIX, ""       ));
        result.put(ARG_TOPIC_PREFIX, line.getOptionValue        (ARG_TOPIC_PREFIX, "test"     ));
        result.put(ARG_NUM_OF_TOPICS, line.getOptionValue       (ARG_NUM_OF_TOPICS, "1"       ));
        result.put(ARG_NUM_OF_PRODUCERS, line.getOptionValue    (ARG_NUM_OF_PRODUCERS, "1"    ));
        result.put(ARG_MESSAGE_PER_PRODUCER, line.getOptionValue(ARG_MESSAGE_PER_PRODUCER, "1"));
        result.put(ARG_MAX_TIME_TO_WAIT, line.getOptionValue    (ARG_MAX_TIME_TO_WAIT, "25"   ));
        return result;
    }
}
