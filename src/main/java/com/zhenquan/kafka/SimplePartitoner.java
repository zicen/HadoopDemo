package com.zhenquan.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitoner implements Partitioner {
    public SimplePartitoner(VerifiableProperties props) {

    }
    @Override
    public int partition(Object o, int numPartitions) {
        int partition = 0;
        String key = (String) o;
        partition = Math.abs(key.hashCode()) % numPartitions;
        return partition;
    }
}
