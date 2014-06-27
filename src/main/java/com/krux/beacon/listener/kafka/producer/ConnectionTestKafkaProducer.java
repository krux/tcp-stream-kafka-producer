package com.krux.beacon.listener.kafka.producer;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.TCPStreamListenerServer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ConnectionTestKafkaProducer {

    private static Producer<String, String> producer;
    
    private static final Logger log = LoggerFactory.getLogger(ConnectionTestKafkaProducer.class.getName());

    static {

        Properties props = new Properties();

        // all the below properties are set in BeaconListenerServer after
        // parsing the command line options
        props.put("metadata.broker.list", System.getProperty("metadata.broker.list", "localhost:9092"));
        props.put("serializer.class", System.getProperty("serializer.class", "kafka.serializer.StringEncoder"));
        props.put("partitioner.class",
                System.getProperty("partitioner.class", "com.krux.beacon.listener.kafka.producer.SimplePartitioner"));
        props.put("producer.type", "sync");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

    }

    public static void sendTest() {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("TEST_CONN", "", "This is a test");
        producer.send(data);
    }
}