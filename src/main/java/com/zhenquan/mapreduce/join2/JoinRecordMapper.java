package com.zhenquan.mapreduce.join2;

import com.zhenquan.hdfs.copyfile.TextPair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = StringUtils.split(value.toString(), "\t");

        if (arr.length == 3) {
            System.out.println("JoinRecordMapper arr[0]:"+arr[0]+",arr[1]:"+arr[1]+",arr[2]:"+arr[2]);
            context.write(new TextPair(arr[0], "1"), new Text(arr[1] + "\t" + arr[2]));
        }
    }
}
