package com.krux.beacon.listener.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ConnectionTestKafkaProducer {

    private static Producer<String, String> producer;

    static {

        Properties props = new Properties();

        // all the below properties are set in BeaconListenerServer after
        // parsing the command line options
        props.put("metadata.broker.list", System.getProperty("metadata.broker.list", "localhost:9092"));
        props.put("serializer.class", System.getProperty("serializer.class", "kafka.serializer.StringEncoder"));
        props.put("partitioner.class",
                System.getProperty("partitioner.class", "com.krux.beacon.listener.kafka.producer.SimplePartitioner"));
        props.put("producer.type", System.getProperty("producer.type", "sync"));

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

    }

    public static void send() {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("TEST_CONN", "", "This is a test");
        try {
            producer.send(data);
        } catch ( Exception e ) {
            //reset __status message and close tcp port
        }
    }
}