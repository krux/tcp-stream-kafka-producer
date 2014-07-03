package com.krux.beacon.listener.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

    private static Producer<String, String> PRODUCER;

    static {

        Properties props = new Properties();

        // all the below properties are set in TCPStreamListenerServer after
        // parsing the command line options
        props.put("metadata.broker.list", System.getProperty("metadata.broker.list", "localhost:9092"));
        props.put("serializer.class", System.getProperty("serializer.class", "kafka.serializer.StringEncoder"));
        props.put("partitioner.class",
                System.getProperty("partitioner.class", "com.krux.beacon.listener.kafka.producer.SimplePartitioner"));
        props.put("request.required.acks", System.getProperty("request.required.acks", "1"));
        props.put("producer.type", System.getProperty("producer.type", "async"));

        props.put("request.timeout.ms", System.getProperty("request.timeout.ms", "10000"));
        props.put("compression.codec", System.getProperty("compression.codec", "none"));
        props.put("message.send.max.retries", System.getProperty("message.send.max.retries", "3"));
        props.put("retry.backoff.ms", System.getProperty("retry.backoff.ms", "100"));
        props.put("queue.buffering.max.ms", System.getProperty("queue.buffering.max.ms", "5000"));
        props.put("queue.buffering.max.messages", System.getProperty("queue.buffering.max.messages", "10000"));
        props.put("queue.enqueue.timeout.ms", System.getProperty("queue.enqueue.timeout.ms", "-1"));
        props.put("batch.num.messages", System.getProperty("batch.num.messages", "200"));
        props.put("client.id", System.getProperty("client.id", ""));
        props.put("send.buffer.bytes", System.getProperty("send.buffer.bytes", String.valueOf(100 * 1024)));

        ProducerConfig config = new ProducerConfig(props);
        PRODUCER = new Producer<String, String>(config);

    }

    public static void send(String topic, String message) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, "", message);
        PRODUCER.send(data);
    }
}