package com.krux.beacon.listener;

import static com.codahale.metrics.MetricRegistry.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
    private static final Map<String,Object> rps = Collections.synchronizedMap(
            new HashMap<String,Object>());
    private static Map<String,Meter> rqsMeters = null;
    
    private static final MetricRegistry metrics = new MetricRegistry();
    
    static {    
        StdHttpServerHandler.addAdditionalStatus( "latestTopicProcessingTimesNanoSec", lastTopicTimes );
        StdHttpServerHandler.addAdditionalStatus( "topicQps", rps );
    }

    private List<String> _topics;

    public BeaconListenerHandler(List<String> topics) {
        _topics = topics;
        
        if ( rqsMeters == null ) {
            rqsMeters = Collections.synchronizedMap( new HashMap<String,Meter>() );
            for ( String topic : _topics ) {
                rqsMeters.put( topic, metrics.meter(name(BeaconListenerHandler.class, topic + "_requests")));
            }
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {
        
        long start = System.currentTimeMillis();
        long startNs = System.nanoTime();

        // All we do here is take the incoming message and plop it onto the
        // configured kafka topic(s). Too easy
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received message: " + request);
        }
        for (String topic : _topics) {
            try {
                KafkaProducer.send(topic, request);
                long time = System.currentTimeMillis() - start;
                long timeNs = System.nanoTime() - startNs;
                lastTopicTimes.put( topic, timeNs );
                KruxStdLib.STATSD.time("message_processed_" + topic, time);
                
                rqsMeters.get( topic ).mark();
                Map<String,Object> qpsMap = new HashMap<String,Object>();
                Meter m = rqsMeters.get( topic );
                qpsMap.put( "count", m.getCount() );
                qpsMap.put( "1_min_rate", m.getOneMinuteRate() );
                qpsMap.put( "5_min_rate", m.getFiveMinuteRate() );
                qpsMap.put( "15_min_rate", m.getFifteenMinuteRate() );
                qpsMap.put( "mean_rate", m.getMeanRate() );
                
                rps.put( topic, qpsMap );
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