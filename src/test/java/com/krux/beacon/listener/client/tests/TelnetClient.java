/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.krux.beacon.listener.client.tests;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.lang3.RandomStringUtils;

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
            
            for (int i = 0; i < 10000; i++ ) {
                String line = RandomStringUtils.random(500, true, true);

                // Sends the received line to the server.
                lastWriteFuture = ch.writeAndFlush(line + "\r\n");
            }
            
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++ ) {
                String line = RandomStringUtils.random(500, true, true);

                // Sends the received line to the server.
                lastWriteFuture = ch.writeAndFlush(line + "\r\n");
            }
            long time = System.currentTimeMillis() - start;

           System.out.println( time );
            // Wait until all messages are flushed before closing the channel.
            if (lastWriteFuture != null) {
                lastWriteFuture.sync();
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}