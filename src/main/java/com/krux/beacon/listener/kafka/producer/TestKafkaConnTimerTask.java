package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;

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

    public TestKafkaConnTimerTask(String topic, int decoderFrameSize) {
        _testTopic = topic;
        _decoderFrameSize = decoderFrameSize;
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
                TCPStreamListenerServer.startListeners(_testTopic, _decoderFrameSize);
            }

        } catch (Exception e) {
            LOG.error("Cannot send test message", e);
            KruxStdLib.STATSD.count("heartbeat_topic_failure");
            if (TCPStreamListenerServer.IS_RUNNING.get()) {
                KruxStdLib.STATSD.count("listener_stopping_test_topic_failure");
                LOG.error("Stopping listeners");
                for (BeaconListener listener : TCPStreamListenerServer.LISTENERS) {
                    listener.stop();
                }
                TCPStreamListenerServer.IS_RUNNING.set(false);
                StdHttpServerHandler.setStatusCodeAndMessage(AppState.FAILURE, "Test message failed, listeners stopped: "
                        + e.getMessage());
            } else {
                LOG.info("Listeners not running, will not attempt to start them");
            }
        }

    }
}
