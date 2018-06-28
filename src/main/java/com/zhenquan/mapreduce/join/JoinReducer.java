package com.zhenquan.mapreduce.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text>{
	 protected void reduce(TextPair key, Iterable< Text> values,Context context) throws IOException,InterruptedException{
	        Iterator< Text> iter = values.iterator();
	        Text stationName = new Text(iter.next());//气象站名称
	        while(iter.hasNext()){
	            Text record = iter.next();//天气记录的每条数据
	            Text outValue = new Text(stationName.toString()+"\t"+record.toString());
	            System.out.println("reduce:"+key.getFirst()+",value:"+outValue);
	            context.write(key.getFirst(),outValue);
	        }
	    }  
	
}
