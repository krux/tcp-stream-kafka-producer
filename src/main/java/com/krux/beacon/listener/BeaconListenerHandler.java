package com.krux.beacon.listener;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.kafka.producer.KafkaProducer;

/**
 * Handles a server-side channel.
 */
@Sharable
public class BeaconListenerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory.getLogger(BeaconListenerServer.class.getName());

    private List<String> _topics;

    public BeaconListenerHandler(List<String> topics) {
        _topics = topics;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request) throws Exception {

        // put message on each topic
        // list comes from ?
        // static map? List passed into constructor?

        // Generate and write a response.
        log.info("Received message: " + request);
        for (String topic : _topics) {
            // push message to topic
            log.info("  **Would place on topic: " + topic);
            KafkaProducer.send( topic, request );
        }

        // String response;
        // boolean close = false;
        // if (request.isEmpty()) {
        // response = "Please type something.\r\n";
        // } else if ("bye".equals(request.toLowerCase())) {
        // response = "Have a good day!\r\n";
        // close = true;
        // } else {
        // response = "Did you say '" + request + "'?\r\n";
        // }
        //
        // // We do not need to write a ChannelBuffer here.
        // // We know the encoder inserted at TelnetPipelineFactory will do the
        // conversion.
        // ChannelFuture future = ctx.write(response);
        //
        // // Close the connection after sending 'Have a good day!'
        // // if the client has sent 'bye'.
        // if (close) {
        // future.addListener(ChannelFutureListener.CLOSE);
        // }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}