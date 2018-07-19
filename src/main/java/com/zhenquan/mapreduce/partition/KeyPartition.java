package com.zhenquan.mapreduce.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartition extends Partitioner<TextInt, IntWritable> {
    @Override
    public int getPartition(TextInt textInt, IntWritable intWritable, int numPartitions) {
        return (textInt.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
