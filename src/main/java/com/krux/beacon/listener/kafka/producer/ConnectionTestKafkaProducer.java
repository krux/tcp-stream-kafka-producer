package com.krux.beacon.listener.kafka.producer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ConnectionTestKafkaProducer {

    private static Producer<String, String> PRODUCER;

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTestKafkaProducer.class.getName());

    private static String HOSTNAME;

    static {

        Properties props = new Properties();

        // metadata broker list is set in TCPStreamListenerServer
        props.put("metadata.broker.list", System.getProperty("metadata.broker.list", "localhost:9092"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.krux.kafka.producer.SimplePartitioner");
        props.put("producer.type", "sync");
        props.put("message.send.max.retries", "0");

        ProducerConfig config = new ProducerConfig(props);
        PRODUCER = new Producer<String, String>(config);
        try {
            HOSTNAME = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            HOSTNAME = "UnknownHost";
        }
    }

    public static void sendTest(String topic) {
        long start = System.currentTimeMillis();
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, "", "This is a test - " + HOSTNAME);
        LOG.debug("Sending test message to {}", topic);
        PRODUCER.send(data);
        LOG.debug("SENT test message to {}", topic);
        long time = System.currentTimeMillis() - start;
        KruxStdLib.STATSD.time("test_message_sent", time);
    }
}