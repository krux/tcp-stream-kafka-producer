package com.krux.beacon.listener;

import static com.codahale.metrics.MetricRegistry.name;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.jr.ob.JSON;
import com.krux.beacon.listener.kafka.producer.DroppedMessagesTimerTask;
import com.krux.kafka.producer.KafkaProducer;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

/**
 * Handles a server-side channel.
 */
@Sharable
public class BeaconListenerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconListenerHandler.class.getName());
    private static final Map<String, Long> lastTopicTimes = Collections.synchronizedMap(new HashMap<String, Long>());
    private static final Map<String, Object> rps = Collections.synchronizedMap(new HashMap<String, Object>());
    private static Map<String, Meter> rqsMeters = Collections.synchronizedMap(new HashMap<String, Meter>());

    private static final MetricRegistry metrics = new MetricRegistry();

    private static final Map<String, KafkaProducer> producers = Collections
            .synchronizedMap(new HashMap<String, KafkaProducer>());
    private static final Meter droppedMessasgesMeter = metrics.meter(name(BeaconListenerHandler.class, "dropped_messages"));

    private OptionSet _options;

    static {
        StdHttpServerHandler.addAdditionalStatus("last_msg_proc_time_nsec", lastTopicTimes);
        StdHttpServerHandler.addAdditionalStatus("topic_message_rates", rps);
    }

    private List<String> _topics;

    public BeaconListenerHandler(List<String> topics, OptionSet options) {
        _topics = topics;
        _options = options;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {

        long startNs = System.nanoTime();
        long start = System.currentTimeMillis();
        // let's see how often this happens
        try {
            request = request.replace("\\x", "\\u00"); // "hex" escaping
                                                       // technically not
                                                       // allowed in JSON, only
                                                       // unicode
            JSON.std.mapFrom(request);
        } catch (Exception e) {
            LOG.error("Cannot parse message as JSON", new String(request), e);
            long time = System.currentTimeMillis() - start;
            String mainTopic = _topics.get(0);
            KruxStdLib.STATSD.time("message_json_parse_error." + mainTopic, time);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received message: {}", request);
        }

        LOG.debug("ALWAYS_ACCEPT_STREAMS: {}", TCPStreamListenerServer.ALWAYS_ACCEPT_STREAMS);
        LOG.debug("SEND_TO_KAFKA: {}", TCPStreamListenerServer.SEND_TO_KAFKA);

        if (TCPStreamListenerServer.SEND_TO_KAFKA) {
            // each port of incoming data can be configured to push to more than
            // one topic
            for (String topic : _topics) {
                try {
                    KafkaProducer producer = producers.get(topic);
                    if (producer == null) {
                        // create new
                        // producer = new KafkaProducer( topic );
                        producer = new KafkaProducer(_options, topic);
                        producers.put(topic, producer);
                    }
                    producer.send(topic, request);
                    long timeNs = System.nanoTime() - startNs;
                    lastTopicTimes.put(topic, timeNs);

                    Map<String, Object> qpsMap = new HashMap<String, Object>();
                    Meter m = rqsMeters.get(topic);
                    if (m == null) {
                        m = metrics.meter(name(BeaconListenerHandler.class, topic + "_requests"));
                        rqsMeters.put(topic, m);
                    }
                    m.mark();
                    qpsMap.put("count", m.getCount());
                    qpsMap.put("1_min_rate", m.getOneMinuteRate());
                    qpsMap.put("5_min_rate", m.getFiveMinuteRate());
                    qpsMap.put("15_min_rate", m.getFifteenMinuteRate());
                    qpsMap.put("mean_rate", m.getMeanRate());

                    rps.put(topic, qpsMap);
                } catch (Exception e) {
                    LOG.error("Error trying to send message", e);
                    long time = System.currentTimeMillis() - start;
                    KruxStdLib.STATSD.time("message_error." + topic, time);
                }
            }
            long time = System.currentTimeMillis() - start;
            KruxStdLib.STATSD.time("message_processed_all", time);
        } else {
            long time = System.currentTimeMillis() - start;
            DroppedMessagesTimerTask.droppedMessages.incrementAndGet();
            Map<String, Object> errorMap = new HashMap<String, Object>();
            droppedMessasgesMeter.mark();
            errorMap.put("count", droppedMessasgesMeter.getCount());
            errorMap.put("1_min_rate", droppedMessasgesMeter.getOneMinuteRate());
            errorMap.put("5_min_rate", droppedMessasgesMeter.getFiveMinuteRate());
            errorMap.put("15_min_rate", droppedMessasgesMeter.getFifteenMinuteRate());
            errorMap.put("mean_rate", droppedMessasgesMeter.getMeanRate());
            StdHttpServerHandler.addAdditionalStatus("dropped_message_rates", errorMap);
            StdHttpServerHandler.addAdditionalStatus("msgs_dropped_since_process_start",
                    DroppedMessagesTimerTask.droppedMessages.incrementAndGet());
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Error during message handling", cause);
        KruxStdLib.STATSD.count("message_processed_error_all");
        // cause.printStackTrace();
        // ctx.close();
    }
}