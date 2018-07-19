package com.zhenquan.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<TextPair, Text>{

	@Override
	public int getPartition(TextPair key,Text value,int numPartitions){
        System.out.println("numPartitions:"+numPartitions);
        return (key.getFirst().hashCode()&Integer.MAX_VALUE)% numPartitions;
    }


}
