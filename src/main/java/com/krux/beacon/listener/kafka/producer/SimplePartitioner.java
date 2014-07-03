package com.krux.beacon.listener.kafka.producer;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

    private static Random _r = new Random();

    /* may use this later */
    public SimplePartitioner(VerifiableProperties props) {

    }

    @Override
    public int partition(Object obj, int a_numPartitions) {
        return _r.nextInt(a_numPartitions);
    }

}