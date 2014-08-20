package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;

import kafka.producer.ProducerStats;
import kafka.producer.ProducerStatsRegistry;

import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class DroppedMessagesTimerTask extends TimerTask {

    private static long droppedMessages = 0;
    private static int reportCount = 0;

    @Override
    public void run() {
        ProducerStats pstats = ProducerStatsRegistry.getProducerStats(System.getProperty("client.id", ""));
        long droppedMessageCount = (long) pstats.failedSendRate().count();
        Integer batchSize = Integer.parseInt(System.getProperty("batch.num.messages"));
        Long latestDropped = droppedMessageCount * batchSize;

        if ( latestDropped > droppedMessages ) {
            //more messages have been dropped, report new value
            reportCount = 0;
            StdHttpServerHandler.addAdditionalStatus("dropped_messages", (latestDropped - droppedMessages));
            KruxStdLib.STATSD.gauge("dropped_messages", (latestDropped - droppedMessages));
            
        } else if ( latestDropped == droppedMessages ) {
            reportCount++;
            if ( reportCount == 3 ) {
                StdHttpServerHandler.addAdditionalStatus("dropped_messages", (latestDropped - droppedMessages));
                KruxStdLib.STATSD.gauge("dropped_messages", (latestDropped - droppedMessages));
                reportCount = 0;
            }
        }
        droppedMessages = latestDropped;

        
        

    }

}
