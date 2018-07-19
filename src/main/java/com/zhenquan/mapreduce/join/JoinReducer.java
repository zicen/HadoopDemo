package com.zhenquan.mapreduce.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text>{
	 protected void reduce(TextPair key, Iterable< Text> values,Context context) throws IOException,InterruptedException{
	        Iterator< Text> iter = values.iterator();
	        Text stationName = new Text(iter.next());//气象站名称,因为我们在分组的时候进行了二次排序，所以肯定是0先进来，也就是station map先进来，所以第一个value就是stationName
	        while(iter.hasNext()){
				//天气记录的每条数据  这里为什么是天气记录的每条数据？？？因为我们使用了stationId作为分区，所以这里我们获取的数据就是stationId相同的数据
//				key=011990-99999	value=195005150700	0
//				011990-99999	195005151200	22
//				011990-99999	195005151800	-11
	            Text record = iter.next();

	            Text outValue = new Text(stationName.toString()+"\t"+record.toString());
	            System.out.println("key:"+key.getFirst()+",value:"+outValue);
	            context.write(key.getFirst(),outValue);
	        }
	    }  
	
}
