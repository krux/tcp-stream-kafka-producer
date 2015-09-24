package com.krux.beacon.listener.client.tests;

import org.apache.commons.lang3.RandomStringUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Simplistic telnet client.
 */
public final class TelnetClient {

    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = 32344;

    public static void main(String[] args) throws Exception {

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new TelnetClientInitializer());

            // Start the connection attempt.
            Channel ch = b.connect(HOST, PORT).sync().channel();

            // Read commands from the stdin.
            ChannelFuture lastWriteFuture = null;

            for (int i = 0; i < 1000; i++) {
                String line = RandomStringUtils.random(100, true, true);

                // Sends the received line to the server.
                lastWriteFuture = ch.writeAndFlush(line + "\r\n");
            }

            long start = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                String line = RandomStringUtils.random(100, true, true);

                // Sends the received line to the server.
                lastWriteFuture = ch.writeAndFlush(line + "\r\n");
                System.out.println("Sent " + i);
                Thread.sleep(100);
            }
            long time = System.currentTimeMillis() - start;

            System.out.println(time);
            // Wait until all messages are flushed before closing the channel.
            if (lastWriteFuture != null) {
                lastWriteFuture.sync();
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}