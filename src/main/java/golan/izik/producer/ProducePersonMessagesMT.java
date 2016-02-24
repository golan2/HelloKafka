package golan.izik.producer;

import golan.izik.Utils;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by golaniz on 22/02/2016.
 */
public class ProducePersonMessagesMT extends ProduceMessagesMultiThreads<ProducePersonMessagesMT.Person> {

    public static void main(String[] args) throws ParseException, InterruptedException {
        HashMap<String, String> map = Utils.parseCommandLineArgs(args);
        ProducePersonMessagesMT runner = new ProducePersonMessagesMT(map);
        runner.run();
    }

    protected ProducePersonMessagesMT(HashMap<String, String> map) {
        super(map);
    }

    @Override
    protected ProducerTask.KeyGenerator<Person> getKeyGen() {
        return (message, topicName, producerId) -> topicName + "_" + producerId + "_" + message.getLastName();
    }

    @Override
    protected List<Person> generateMessages(String topicName, String messagePrefix, int messagesCount) {
        ArrayList<Person> result = new ArrayList<>();
        for (int i = 0; i < messagesCount; i++) {
            result.add(new Person(messagePrefix, topicName+"_"+i));
        }
        return result;
    }


    //TODO: org.apache.kafka.common.errors.SerializationException: Can't convert value of class golan.izik.producer.ProducePersonMessagesMT$Person to class org.apache.kafka.common.serialization.StringSerializer specified in value.serializer
    public static class Person implements Serializable{
        private final String firstName;
        private final String lastName;

        public Person(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    '}';
        }
    }
}
