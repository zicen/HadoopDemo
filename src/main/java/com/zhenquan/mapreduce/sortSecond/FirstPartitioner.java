package com.zhenquan.mapreduce.sortSecond;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPair,IntWritable> {
    @Override
    public int getPartition(IntPair intPair, IntWritable intWritable, int numPartitions) {
        return Math.abs(intPair.first * 127) % numPartitions;
    }
}
