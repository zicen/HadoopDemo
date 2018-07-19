package com.zhenquan.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;


public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text>{
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] arr = StringUtils.split(value.toString(),"\t");
		if (arr.length == 3) {
			System.out.println("JoinRecordMapper key:"+arr[0]+",value:"+arr[1]+"\t"+arr[2]);
			context.write(new TextPair(arr[0], "1"), new Text(arr[1]+"\t"+arr[2]));
		}
	};
	
	
	@Test
	public void testSplit(){
		String aString = "012650-99999	194903241200	111";
		String[] arr = StringUtils.split(aString,"\\s+");
		for (int i = 0; i < arr.length; i++) {
			System.out.println("JoinRecordMapper arr："+arr[i]);
		}
		String[] arr2 = StringUtils.split(aString,"\t");
		for (int i = 0; i < arr2.length; i++) {
			System.out.println("JoinRecordMapper arr2："+arr2[i]);
		}
		String[] tokens = aString.split("\t");//使用分隔符\t，将数据解析为数组 tokens
		for (int i = 0; i < tokens.length; i++) {
			System.out.println("JoinRecordMapper tokens"+tokens[i]);
		}
	}
}
