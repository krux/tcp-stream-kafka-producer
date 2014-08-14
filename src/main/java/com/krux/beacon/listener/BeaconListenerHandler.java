package com.krux.beacon.listener;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.kafka.producer.KafkaProducer;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

/**
 * Handles a server-side channel.
 */
@Sharable
public class BeaconListenerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconListenerHandler.class.getName());
    private static final Map<String,Long> lastTopicTimes = Collections.synchronizedMap(
            new HashMap<String,Long>());
    
    static {    
        StdHttpServerHandler.addAdditionalStatus( "topicProcessingTimesMs", lastTopicTimes );
    }

    private List<String> _topics;

    public BeaconListenerHandler(List<String> topics) {
        _topics = topics;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {
        
        long start = System.currentTimeMillis();

        // All we do here is take the incoming message and plop it onto the
        // configured kafka topic(s). Too easy
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received message: " + request);
        }
        for (String topic : _topics) {
            try {
                KafkaProducer.send(topic, request);
                long time = System.currentTimeMillis() - start;
                lastTopicTimes.put( topic, time );
                KruxStdLib.STATSD.time("message_processed_" + topic, time);
            } catch ( Exception e ) {
                long time = System.currentTimeMillis() - start;
                KruxStdLib.STATSD.time("message_error_" + topic, time);                
            }
        }

        long time = System.currentTimeMillis() - start;
        KruxStdLib.STATSD.time("message_processed_all", time);

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