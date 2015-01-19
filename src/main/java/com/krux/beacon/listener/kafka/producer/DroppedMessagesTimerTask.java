package com.krux.beacon.listener.kafka.producer;

import java.util.TimerTask;

import kafka.producer.ProducerStats;
import kafka.producer.ProducerStatsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class DroppedMessagesTimerTask extends TimerTask {
    
    private static final Logger LOG = LoggerFactory.getLogger(DroppedMessagesTimerTask.class.getName());

    private static long droppedMessages = 0;
    private static long reportedDropped = 0;
    private static int reportCount = 0;

    @Override
    public void run() {
        ProducerStats pstats = ProducerStatsRegistry.getProducerStats(System.getProperty("client.id", ""));
        long droppedMessageCount = (long) pstats.failedSendRate().count();
        Integer batchSize = Integer.parseInt(System.getProperty("batch.num.messages"));
        Long latestDropped = droppedMessageCount * batchSize;
        LOG.warn( "latestDropped: " + latestDropped + ", droppedMessages: " + droppedMessages);
        if ( latestDropped > droppedMessages ) {
            reportedDropped = latestDropped - droppedMessages; //more messages have been dropped, report new value
            reportCount = 0;
            
        } else if ( latestDropped == droppedMessages ) {
            LOG.warn( "reportCount: " + reportCount );
            reportCount++;
            if ( reportCount == 2 ) {
                reportedDropped = 0;
                reportCount = 0;
            }
        }  
        StdHttpServerHandler.addAdditionalStatus("dropped_messages", reportedDropped);
        KruxStdLib.STATSD.gauge("dropped_messages", reportedDropped);
        droppedMessages = latestDropped;
    }

}
