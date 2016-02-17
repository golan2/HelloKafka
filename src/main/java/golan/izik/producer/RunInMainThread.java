package golan.izik.producer;

import golan.izik.Utils;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by golaniz on 09/02/2016.
 */
public class RunInMainThread {

    public static void main(String[] args) throws ParseException {
        HashMap<String, String> map = Utils.parseCommandLineArgs(args);

        Utils.consolog(Utils.getCurrentTimeStamp() + " Run ProducerTask...");

        ArrayList<String> messages = new ArrayList<>(1);
        messages.add(map.get(Utils.ARG_MESSAGE_PREFIX)+new Random().nextInt(1000));
        ProducerTask task = new ProducerTask(map.get(Utils.ARG_SERVER), messages, map.get(Utils.ARG_TOPIC_PREFIX));
        task.run();
        Utils.consolog(Utils.getCurrentTimeStamp() + " Done!");
    }
}
