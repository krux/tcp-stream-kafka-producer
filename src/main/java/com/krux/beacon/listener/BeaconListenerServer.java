package com.krux.beacon.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class BeaconListenerServer {

    private static final Logger log = LoggerFactory.getLogger(BeaconListenerServer.class.getName());

    public static Map<Integer, List<String>> portToTopicsMap = new HashMap<Integer, List<String>>();
    public static List<Thread> servers = new ArrayList<Thread>();

    public static void main(String[] args) {

        // handle a couple custom cli-params
        OptionParser parser = new OptionParser();

        OptionSpec<String> portTopicMappings = parser
                .accepts("port.topic", "The port->topic mappings (ex: 1234:topic1[,topic2])").withOptionalArg()
                .ofType(String.class);
        OptionSpec<String> kafkaBrokers = parser
                .accepts("metadata.broker.list", "A comma-delimitted list of kafka brokers in host:port format.")
                .withOptionalArg().ofType(String.class).defaultsTo("localhost:9092");
        OptionSpec<Integer> kafkaAckType = parser
                .accepts("request.required.acks",
                        "The type of ack the broker will return to the client.  See https://kafka.apache.org/documentation.html#producerconfigs")
                .withOptionalArg().ofType(Integer.class).defaultsTo(1);
        OptionSpec<String> producerType = parser.accepts("producer.type", "'sync' or 'async'").withOptionalArg()
                .ofType(String.class).defaultsTo("async");

        // give parser to KruxStdLib so it can add our params to the reserved
        // list
        KruxStdLib.setOptionParser(parser);
        OptionSet options = KruxStdLib.initialize(args);

        // parse the configured port -> topic mappings, put in global hashmap
        Map<OptionSpec<?>, List<?>> optionMap = options.asMap();
        List<?> portTopicMap = optionMap.get(portTopicMappings);

        for (Object mapping : portTopicMap) {
            String mappingString = (String) mapping;
            String[] parts = mappingString.split(":");
            Integer port = Integer.parseInt(parts[0]);
            String[] topics = parts[1].split(",");

            List<String> topicList = new ArrayList<String>();
            for (String topic : topics) {
                topicList.add(topic);
            }
            portToTopicsMap.put(port, topicList);
        }

        // these are picked up by the KafkaProducer class
        System.setProperty("metadata.broker.list", (String) optionMap.get(kafkaBrokers).get(0));
        System.setProperty("request.required.acks", String.valueOf((Integer) optionMap.get(kafkaAckType).get(0)));
        System.setProperty("producer.type", (String) optionMap.get(producerType).get(0));

        // ok, mappings and properties handled. Now, start tcp server on each
        // port
        for (Map.Entry<Integer, List<String>> entry : portToTopicsMap.entrySet()) {
            StringBuilder sb = new StringBuilder();
            for (String topic : entry.getValue()) {
                sb.append(topic);
                sb.append(", ");
            }
            log.info("Starting listener on port " + entry.getKey() + " for topics " + sb.toString());
            BeaconListener listener = new BeaconListener(entry.getKey(), entry.getValue());
            Thread t = new Thread(listener);
            servers.add(t);
            t.start();
        }

        for (Thread t : servers) {
            try {
                // unless something goes horribly wrong and doesn't get caught
                // somewhere downstream, we'll never make it past the following
                // line
                t.join();
            } catch (InterruptedException e) {
                log.error("Error after starting server", e);
            }
        }

        System.out.println("Closed.");

    }

}
