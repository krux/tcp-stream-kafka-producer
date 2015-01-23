package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.stdlib.KruxStdLib;

public class DroppedMessagesTimerTask extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(DroppedMessagesTimerTask.class.getName());

    public static AtomicLong droppedMessages = new AtomicLong(0);

    @Override
    public void run() {
        LOG.debug("Reporting {} dropped messages", droppedMessages.get());
        KruxStdLib.STATSD.gauge("dropped_messages", droppedMessages.get());
        droppedMessages.set(0);

    }

}
