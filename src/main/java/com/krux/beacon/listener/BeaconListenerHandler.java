package com.krux.beacon.listener;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.kafka.producer.KafkaProducer;
import com.krux.stdlib.KruxStdLib;

/**
 * Handles a server-side channel.
 */
@Sharable
public class BeaconListenerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory.getLogger(BeaconListenerHandler.class.getName());

    private List<String> _topics;

    public BeaconListenerHandler(List<String> topics) {
        _topics = topics;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {

        long start = System.currentTimeMillis();

        // All we do here is take the incoming message and plop it onto the
        // configured kafka topic(s). Too easy
        if (log.isDebugEnabled()) {
            log.debug("Received message: " + request);
        }
        for (String topic : _topics) {
            KafkaProducer.send(topic, request);
        }

        long time = System.currentTimeMillis() - start;
        KruxStdLib.statsd.time("beacon-listener-message", time);

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error during message handling", cause);
        KruxStdLib.statsd.count( "beacon-listener-message-error" );
        // cause.printStackTrace();
        // ctx.close();
    }
}