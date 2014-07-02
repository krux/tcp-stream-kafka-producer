package com.krux.beacon.listener;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeaconListener implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BeaconListener.class.getName());

    private int _port;
    private List<String> _topics;
    private ServerBootstrap _b;
    EventLoopGroup _bossGroup;
    EventLoopGroup _workerGroup;
    ChannelFuture _cf;

    public BeaconListener(Integer port, List<String> topics) {
        _port = port;
        _topics = topics;
    }

    @Override
    public void run() {

        _bossGroup = new NioEventLoopGroup(1);
        _workerGroup = new NioEventLoopGroup();
        try {
            _b = new ServerBootstrap();
            _b.group(_bossGroup, _workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new BeaconListenerInitializer(_topics));

            _cf = _b.bind(_port).sync();
            _cf.channel().closeFuture().sync();
            
        } catch (Exception e) {
            log.error("Error running listener server on " + _port, e);
        } finally {
            _bossGroup.shutdownGracefully();
            _workerGroup.shutdownGracefully();
        }

    }
    
    public void stop() {
//        _bossGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
//        _workerGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
        try {
            _bossGroup.shutdownNow();
            _workerGroup.shutdownNow();
            _cf.cancel(true);
        } catch ( Exception e ) {
            log.error( "Error while closing listeners.", e );
        }
    }

}
