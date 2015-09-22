package com.krux.beacon.listener.kafka.producer;

import com.krux.stdlib.KruxStdLib;

import kafka.common.FailedToSendMessageException;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.producer.async.DefaultEventHandler;
import kafka.serializer.Encoder;
import scala.collection.Seq;
import scala.collection.mutable.HashMap;

public class KruxProducerEventHandler extends DefaultEventHandler<String, String> {

    public KruxProducerEventHandler(ProducerConfig config, Partitioner partitioner, Encoder encoder, Encoder keyEncoder,
            ProducerPool producerPool, HashMap topicPartitionInfos) {
        super(config, partitioner, encoder, keyEncoder, producerPool, topicPartitionInfos);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void handle(Seq<KeyedMessage<String, String>> messages) {
        try {
            super.handle(messages);
        } catch (FailedToSendMessageException e) {
            KruxStdLib.STATSD.count("dropped_messages", messages.size());
        }

    }

}
