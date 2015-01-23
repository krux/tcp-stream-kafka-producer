package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;

import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.beacon.listener.BeaconListener;
import com.krux.beacon.listener.TCPStreamListenerServer;
import com.krux.server.http.AppState;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class TestKafkaConnTimerTask extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnTimerTask.class.getName());

    private String _testTopic;
    private int _decoderFrameSize;
    private OptionSet _options;

    public TestKafkaConnTimerTask(String topic, int decoderFrameSize, OptionSet options ) {
        _testTopic = topic;
        _decoderFrameSize = decoderFrameSize;
        _options = options;
    }

    @Override
    public void run() {
        try {

            ConnectionTestKafkaProducer.sendTest(_testTopic);
            LOG.debug("Test message sent successfully");
            KruxStdLib.STATSD.count("heartbeat_topic_success");
            if (!TCPStreamListenerServer.IS_RUNNING.get()) {
                KruxStdLib.STATSD.count("listener_restart");
                LOG.warn("Restarting listeners.");
                TCPStreamListenerServer.RESET_CONN_TIMER.set(true);
                TCPStreamListenerServer.startListeners(_testTopic, _decoderFrameSize, _options);  
            }
            TCPStreamListenerServer.SEND_TO_KAFKA = true;
            StdHttpServerHandler.resetStatusCodeAndMessageOK();

        } catch (Exception e) {
            LOG.error("Cannot send test message", e);
            KruxStdLib.STATSD.count("heartbeat_topic_failure");
            if (TCPStreamListenerServer.IS_RUNNING.get()) {
                
                if ( !TCPStreamListenerServer.ALWAYS_ACCEPT_STREAMS ) {
                	KruxStdLib.STATSD.count("listener_stopping_test_topic_failure");
	                LOG.error("Stopping listeners");
	                for (BeaconListener listener : TCPStreamListenerServer.LISTENERS) {
	                    listener.stop();
	                }
	                TCPStreamListenerServer.IS_RUNNING.set(false);
	                StdHttpServerHandler.setStatusCodeAndMessage(AppState.FAILURE, "Test message failed, listeners stopped");
                } else {
                	TCPStreamListenerServer.SEND_TO_KAFKA = false;
                	KruxStdLib.STATSD.count("listener_stopping_test_topic_failure");
	                StdHttpServerHandler.setStatusCodeAndMessage(AppState.WARNING, "Test message failed, listeners running but dropping messages");                	
                }

            } else {
                LOG.info("Listeners not running, will not attempt to start them");
            }
        }

    }
}
