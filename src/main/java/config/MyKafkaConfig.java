package config;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MyKafkaConfig {
    private final String bootstrapServers;
    private final String topic;


    public MyKafkaConfig(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("bsp", "bootstrap.servers", true, "");
        options.addOption("t", "topic", true, "");

        CommandLineParser parser = new BasicParser();
        final CommandLine commandLine = parser.parse(options, args);
        this.bootstrapServers = commandLine.getOptionValue("bootstrap.servers", "");
        this.topic = commandLine.getOptionValue("topic", "");
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }
}
