package com.krux.beacon.listener.kafka.producer;

import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.BeaconListener;
import com.krux.beacon.listener.TCPStreamListenerServer;

public class TestTimerTask extends TimerTask {
    
    private static final Logger log = LoggerFactory.getLogger(TestTimerTask.class.getName());
    
    private List<BeaconListener> _listeners;

    public TestTimerTask( List<BeaconListener> listeners ) {
        _listeners = listeners;  
    }

    @Override
    public void run() {
        try {
            ConnectionTestKafkaProducer.sendTest();
            log.debug( "Test message sent successfully" );
            if ( !TCPStreamListenerServer.running.get() ) {
                log.info( "Restarting listeners." );
                TCPStreamListenerServer.startListeners();
            }
        } catch ( Exception e ) {
            log.error( "Cannot send test message", e );
            if ( TCPStreamListenerServer.running.get() ) {
                log.error( "Stopping listeners" );
                for ( BeaconListener listener : _listeners ) {
                    listener.stop();
                }
                TCPStreamListenerServer.running.set( false );
            } else {
                log.info( "Listeners not running, will not attempt to start them"  );
            }
        }

    }

}
