package com.zhenquan.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 实现流量汇总并按照流量大小的倒序排序  前提：处理的数据是已经汇总的结果文件
 * @author AllenWoon
 *
 */
public class FlowSumSort {
	
	public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean k = new FlowBean();
		Text  v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] fields = line.split("\t");
			
			String phoNum = fields[0];
			long upFlowSum = Long.parseLong(fields[1]);
			long downFlowSum = Long.parseLong(fields[2]);
			//因为FlowBean的内部实现了compareTo方法，所以会自动的排好序然后分到Reduce上面
			k.set(upFlowSum, downFlowSum);
			v.set(phoNum);
			System.out.println("map key:" + k.toString() + ",value:" + v.toString());
			context.write(k, v);
		}
		
	}
	
	public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> PhoneNum, Context context)
				throws IOException, InterruptedException {
			Text next = PhoneNum.iterator().next();
			System.out.println("reduce key:"+next.toString()+",value:"+bean.toString());
			context.write(next, bean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumSort.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(FlowSumSortMapper.class);
		job.setReducerClass(FlowSumSortReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
		//TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("D:\\hadooptestlocal\\flowsum\\output"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("D:\\hadooptestlocal\\flowsum\\output-sort"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	}

}
