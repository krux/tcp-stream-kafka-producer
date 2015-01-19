package com.krux.beacon.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.kafka.producer.ConnectionTestKafkaProducer;
import com.krux.beacon.listener.kafka.producer.DroppedMessagesTimerTask;
import com.krux.beacon.listener.kafka.producer.TestKafkaConnTimerTask;
import com.krux.kafka.producer.KafkaProducer;
import com.krux.server.http.AppState;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

/**
 * Listens on configurable port(s) and splits incoming TCP stream(s) on newlines
 * into individual Kafka messages, places them on configurable queues. Port ->
 * topic mappings are specified via the --port.topic cl option.
 * 
 * Optionally, an HTTP listener will be started and respond to __status calls
 * with the app's status.
 * 
 * Also optionally, a --heartbeat-topic option may be passed. This topic name
 * will be used for "heartbeat" checks of the Kafka cluster. When that topic
 * cannot be written to, all open TCP listening ports will be closed until the
 * Kafka cluster is available again (allowing upstream handlers to route around
 * this listener).
 * 
 * @author casspc
 * 
 */
public class TCPStreamListenerServer {

    private static final Logger LOG = LoggerFactory.getLogger(TCPStreamListenerServer.class.getName());

    public static Map<Integer, List<String>> PORT_TO_TOPICS_MAP = new HashMap<Integer, List<String>>();
    public static List<Thread> SERVERS = new ArrayList<Thread>();

    public static AtomicBoolean IS_RUNNING = new AtomicBoolean(false);
    public static AtomicBoolean RESET_CONN_TIMER = new AtomicBoolean(false);
    private static Timer CONNECTION_TEST_TIMER = null;
    private static Timer DROPPED_MESSAGES_TIMER = new Timer();
    public static List<BeaconListener> LISTENERS = new ArrayList<BeaconListener>();

    public static boolean USE_KAFKA;

    public static void main(String[] args) throws InterruptedException {

        // handle a couple custom cli-params
        OptionParser parser = new OptionParser();

        OptionSpec<String> portTopicMappings = parser
                .accepts(
                        "port.topic",
                        "The port->topic mappings (ex: 1234:topic1[,topic2])  Specify multiple mappings with multiple cl options.\n  e.g.: --port.topic 1234:topic1[,topic2] --port.topic 4567:topic3[,topic4]")
                .withRequiredArg().ofType(String.class);
        OptionSpec<Integer> decoderFrameSize = parser
                .accepts("krux.decoder.frame.size", "The listener's DelimiterBasedFrameDecoder frame length in bytes")
                .withOptionalArg().ofType(Integer.class).defaultsTo(1024 * 16);
        OptionSpec<String> heartbeatTopic = parser
                .accepts("heartbeat-topic",
                        "The name of a topic to be used for general connection checking, kafka aliveness, etc.")
                .withOptionalArg().ofType(String.class).defaultsTo("");
        OptionSpec keepStreamsOpen = parser.accepts( "always-accept-streams", "Forces the listener to keep incoming stream ports open even when no kafka nodes are reachable. Intended for use during kafka upgrades." );

        // give parser to KruxStdLib so it can add our params to the reserved
        // list
        KruxStdLib.setOptionParser(parser);
        StringBuilder desc = new StringBuilder();
        desc.append("\nKrux Kafka Stream Listener\n");
        desc.append("**************************\n");
        desc.append("Will pass incoming newline-delimitted messages on tcp streams to mapped Kafka topics.\n");
        
        KafkaProducer.addStandardOptionsToParser(parser);
        OptionSet options = KruxStdLib.initialize(desc.toString(), args);

        // parse the configured port -> topic mappings, put in global hashmap
        Map<OptionSpec<?>, List<?>> optionMap = options.asMap();
        List<?> portTopicList = optionMap.get(portTopicMappings);

        for (Object mapping : portTopicList) {
            String mappingString = (String) mapping;
            String[] parts = mappingString.split(":");
            Integer port = Integer.parseInt(parts[0]);
            String[] topics = parts[1].split(",");

            List<String> topicList = new ArrayList<String>();
            for (String topic : topics) {
                topicList.add(topic);
            }
            PORT_TO_TOPICS_MAP.put(port, topicList);
            StdHttpServerHandler.addAdditionalStatus("port_mappings", PORT_TO_TOPICS_MAP);
        }

        StdHttpServerHandler.addAdditionalStatus("client_id", options.valueOf("client.id"));

        // start a timer that will check every N ms to see if test messages
        // can be sent to kafka. If so, then start our listeners
        String testTopic = options.valueOf(heartbeatTopic);
        USE_KAFKA = !options.has( keepStreamsOpen );
        try {
            if (testTopic != null && !testTopic.trim().equals("") && USE_KAFKA) {
                ConnectionTestKafkaProducer.sendTest(options.valueOf(heartbeatTopic));
                startListeners(testTopic, options.valueOf(decoderFrameSize), options);
            } else {
                startListeners(testTopic, options.valueOf(decoderFrameSize), options);
            }
        } catch (Exception e) {
            StdHttpServerHandler.setStatusCodeAndMessage(AppState.FAILURE, "Cannot start listeners: " + e.getMessage());
            System.err.println("Cannot start listeners.");
            LOG.error("Cannot start listeners", e);
            startConnChecker(testTopic, options.valueOf(decoderFrameSize), options);
        }

        // populate the std lib status map with port -> topic configurations
        StdHttpServerHandler.addAdditionalStatus("version", KruxStdLib.APP_VERSION);

        // Jos doesn't want this thing to close even if no port mappings are
        // specified. Hmm.
        // for now, just hang indefinitely
        if (SERVERS.size() <= 1) {
            System.err.println("No listeners started.  See previous errors.");
            StdHttpServerHandler.setStatusCodeAndMessage(AppState.FAILURE, "No listeners started.");
            do {
                Thread.sleep(1000);
            } while (true);
        }

        System.out.println("Closed.");

    }

    public static void startListeners(String testTopic, Integer decoderFrameSize, OptionSet options) {
        // ok, mappings and properties handled. Now, start tcp server on each
        // port

        if (!TCPStreamListenerServer.IS_RUNNING.get()) {
            LISTENERS = new ArrayList<BeaconListener>();
            SERVERS.clear();

            for (Map.Entry<Integer, List<String>> entry : PORT_TO_TOPICS_MAP.entrySet()) {
                StringBuilder sb = new StringBuilder();
                for (String topic : entry.getValue()) {
                    sb.append(topic);
                    sb.append(", ");
                }
                LOG.info("Starting listener on port " + entry.getKey() + " for topics " + sb.toString());
                BeaconListener listener = new BeaconListener(entry.getKey(), entry.getValue(), decoderFrameSize, options);
                LISTENERS.add(listener);
                Thread t = new Thread(listener);
                SERVERS.add(t);
                t.start();
            }

            TCPStreamListenerServer.IS_RUNNING.set(true);

            if ( USE_KAFKA ) {
            	startConnChecker(testTopic, decoderFrameSize, options);
            	StdHttpServerHandler.addAdditionalStatus( "sendingMessagesToKafka", true );
            } else {
            	StdHttpServerHandler.addAdditionalStatus( "sendingMessagesToKafka", false );
            }
            StdHttpServerHandler.resetStatusCodeAndMessageOK();

            for (Thread t : SERVERS) {
                try {
                    // unless something goes horribly wrong and doesn't get
                    // caught
                    // somewhere downstream, we'll never make it past the
                    // following
                    // line
                    t.join();
                } catch (InterruptedException e) {
                    LOG.error("Error after starting server", e);
                }
            }
        }
    }

    private static void startConnChecker(String testTopic, Integer decoderFrameSize, OptionSet options) {
        // start a timer that will check if everything's kosher
        LOG.info("Trying to start the conn checker");
        if (testTopic != null && !testTopic.trim().equals("")) {
            if (CONNECTION_TEST_TIMER == null) {
                LOG.info("testTopic is not null but timer was null");
                CONNECTION_TEST_TIMER = new Timer();
                
                TestKafkaConnTimerTask tt = new TestKafkaConnTimerTask(testTopic, decoderFrameSize, options);
                CONNECTION_TEST_TIMER.schedule(tt, 5000, 1000);

                DroppedMessagesTimerTask dmtt = new DroppedMessagesTimerTask();
                DROPPED_MESSAGES_TIMER.schedule(dmtt, 30000, (60*1000*15));
            } else {
                LOG.info("testTopic is not null AND timer was not null");
                if (RESET_CONN_TIMER.get()) {
                    CONNECTION_TEST_TIMER.cancel();
                    CONNECTION_TEST_TIMER = new Timer();
                    TestKafkaConnTimerTask tt = new TestKafkaConnTimerTask(testTopic, decoderFrameSize, options);
                    CONNECTION_TEST_TIMER.schedule(tt, 5000, 1000);
                    RESET_CONN_TIMER.set(false);
                }
            }
        } else {
            LOG.info("testTopic is null");
        }
    }

}
