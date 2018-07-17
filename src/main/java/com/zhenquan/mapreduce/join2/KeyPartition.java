package com.zhenquan.mapreduce.join2;

import com.zhenquan.hdfs.copyfile.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class KeyPartition extends Partitioner<TextPair,Text> {
    @Override
    public int getPartition(TextPair textPair, Text text, int numPartitions) {
        return (textPair.getFirst().hashCode()&Integer.MAX_VALUE)% numPartitions;
    }
}
