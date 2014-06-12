package com.krux.beacon.listener;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeaconListener implements Runnable {
    
    private static final Logger log = LoggerFactory.getLogger(BeaconListener.class.getName());
    
    private int _port;
    private List<String> _topics;

    public BeaconListener(Integer port, List<String> topics) {
        _port = port;
        _topics = topics;
    }

    @Override
    public void run() {
        
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new BeaconListenerInitializer(_topics));

            b.bind(_port).sync().channel().closeFuture().sync();
        } catch ( Exception e ) {
            log.error( "Error running listener server on " + _port, e );
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

}
