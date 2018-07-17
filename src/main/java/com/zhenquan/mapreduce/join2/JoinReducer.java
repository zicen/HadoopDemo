package com.zhenquan.mapreduce.join2;

import com.zhenquan.hdfs.copyfile.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Text stationName = new Text(iterator.next());//气象站名称
        System.out.println("stationName:" + stationName);
        while (iterator.hasNext()) {
            Text record = iterator.next();//天气记录的每条数据
            Text outvalue = new Text(stationName.toString() + "\t" + record.toString());
            System.out.println("outvalue:" + outvalue);
            context.write(key.getFirst(), outvalue);
        }
    }
}
