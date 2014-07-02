package com.krux.beacon.listener.kafka.producer;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.BeaconListener;
import com.krux.beacon.listener.TCPStreamListenerServer;
import com.krux.server.http.AppState;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class TestKafkaConnTimerTask extends TimerTask {
    
    private static final Logger log = LoggerFactory.getLogger(TestKafkaConnTimerTask.class.getName());
    
    private String _testTopic;

    public TestKafkaConnTimerTask( String topic ) { 
        _testTopic = topic;
    }

    @Override
    public void run() {
        try {
            ConnectionTestKafkaProducer.sendTest(_testTopic);
            log.debug( "Test message sent successfully" );
            KruxStdLib.STATSD.count( "heartbeat_topic_success" );
            if ( !TCPStreamListenerServer.running.get() ) {
                log.info( "Restarting listeners." );
                TCPStreamListenerServer.resetConnTimer.set( true );
                TCPStreamListenerServer.startListeners(_testTopic);
            }
            
        } catch ( Exception e ) {
            log.error( "Cannot send test message", e );
            KruxStdLib.STATSD.count( "heartbeat_topic_failure" );
            if ( TCPStreamListenerServer.running.get() ) {
                log.error( "Stopping listeners" );
                for ( BeaconListener listener : TCPStreamListenerServer._listeners ) {
                    listener.stop();
                }
                TCPStreamListenerServer.running.set( false );
                StdHttpServerHandler.setStatusCodeAndMessage( AppState.FAILURE, "Test message failed, listeners stopped: " + e.getMessage() );
            } else {
                log.info( "Listeners not running, will not attempt to start them"  );
            }
        }

    }

}
