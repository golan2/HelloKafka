package config;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

//--objects 6 --messages 30 --start-date "2018-01-01 00:00:00" --minutes 10


public class MessageCountConf {
    @SuppressWarnings("SpellCheckingInspection")
    private static final DateTimeFormatter DF_MINUTE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final String            DEFAULT_BOOTSTRAP_SERVERS = "kafka-hs:9100";
    private static final String            DEFAULT_TOPIC             = "activity";
    private static final String            DEFAULT_MINUTES_OF_DATA   = "2";
    private static final String            DEFAULT_OBJECTS_COUNT     = "1";
    private static final String            DEFAULT_MSG_PER_MINUTE    = "6";

    private final String        bootstrapServers;
    private final String        topic;
    private final Integer       objects;
    private final Integer       messagesPerMinute;
    private final ZonedDateTime startTime;
    private final Integer       minutesOfData;


    public MessageCountConf(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("b", "bootstrap.servers", true, "Kafka bootstrap servers list");
        options.addOption("t", "topic", true, "Kafka topic to ingest into");
        options.addOption("o", "objects", true, "How many objects to ingest");
        options.addOption("m", "messages", true, "Messages per minute");
        options.addOption("d", "start-time", true, "Date in hour granularity to when to start the ingestion");
        options.addOption("n", "minutes", true, "How manny minutes of data to ingest");


        CommandLineParser parser = new BasicParser();
        final CommandLine commandLine = parser.parse(options, args);
        this.bootstrapServers = commandLine.getOptionValue("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        this.topic = commandLine.getOptionValue("topic", DEFAULT_TOPIC);
        this.objects = Integer.valueOf(commandLine.getOptionValue("objects", DEFAULT_OBJECTS_COUNT));
        final String date = commandLine.getOptionValue("start-time", "");
        if (date.isEmpty()) {
            this.startTime = ZonedDateTime.now();
        }
        else {
            this.startTime = ZonedDateTime.parse(date, DF_MINUTE);
        }
        this.messagesPerMinute = Integer.valueOf(commandLine.getOptionValue("messages", DEFAULT_MSG_PER_MINUTE));
        this.minutesOfData = Integer.valueOf(commandLine.getOptionValue("minutes", DEFAULT_MINUTES_OF_DATA));
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getObjects() {
        return objects;
    }

    public Integer getMessagesPerMinute() {
        return messagesPerMinute;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public Integer getMinutesOfData() {
        return minutesOfData;
    }

    @Override
    public String toString() {
        return "{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", objects=" + objects +
                ", messagesPerMinute=" + messagesPerMinute +
                ", startTime=" + startTime +
                ", minutesOfData=" + minutesOfData +
                '}';
    }
}
