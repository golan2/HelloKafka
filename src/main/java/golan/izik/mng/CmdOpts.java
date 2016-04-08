package golan.izik.mng;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CmdOpts {
    private final Map<String, String> arguments;

    public CmdOpts(String[] args, Map<String, String> params, Set<String> mandatory) throws ParseException {
        Options o = new Options();
        for (String key : params.keySet()) {
            //noinspection AccessStaticViaInstance
            o.addOption(OptionBuilder.hasArgs(1).isRequired(mandatory.contains(key)).create(key));
        }
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        arguments = new HashMap<>(params.size());
        for (String key : params.keySet()) {
            String value = line.getOptionValue(key, params.get(key));
            arguments.put(key,value);
        }
    }

    public String get(String name) {
        return arguments.get(name);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        ArrayList<String> sortedKeys = new ArrayList<>(arguments.keySet());
        sortedKeys.sort(String::compareTo);
        for (String key : sortedKeys) {
            String value = arguments.get(key);
            buf.append("\t[").append(key).append("] => [").append(value).append("]\n");
        }
        return buf.toString();

    }
}
