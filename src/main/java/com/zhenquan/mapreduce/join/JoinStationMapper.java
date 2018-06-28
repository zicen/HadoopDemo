package com.zhenquan.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.view.TextPage;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text>{
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		System.out.println("JoinStationMapper value:"+value.toString());
		  String[] arr = StringUtils.split(value.toString(),"\t");//解析气象站数据
		  for (int i = 0; i < arr.length; i++) {
				System.out.println("JoinStationMapper arr"+i+":"+arr[i].toString());
			}
	    	if(arr.length==2){//满足这种数据格式
	    		//key=气象站id  value=气象站名称
	    		 System.out.println("JoinStationMapper:"+arr[0]+",value:"+arr[1]);
	            context.write(new TextPair(arr[0],"0"),new Text(arr[1]));
	        }
	};
	
}
