package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;

import kafka.producer.ProducerStats;
import kafka.producer.ProducerStatsRegistry;

import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class DroppedMessagesTimerTask extends TimerTask {

    private static long droppedMessages = 0;

    @Override
    public void run() {
        ProducerStats pstats = ProducerStatsRegistry.getProducerStats(System.getProperty("client.id", ""));
        long droppedMessageCount = (long) pstats.failedSendRate().count();
        Integer batchSize = Integer.parseInt(System.getProperty("batch.num.messages"));
        Long latestDropped = droppedMessageCount * batchSize;

        StdHttpServerHandler.addAdditionalStatus("dropped_messages", (latestDropped - droppedMessages));
        droppedMessages = latestDropped;
        KruxStdLib.STATSD.gauge("dropped_messages", (latestDropped - droppedMessages));

    }

}
