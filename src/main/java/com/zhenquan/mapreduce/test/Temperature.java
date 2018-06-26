package com.zhenquan.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Temperature extends Configured implements Tool {
	public static class TemperatureMapper extends Mapper< LongWritable, Text, Text, IntWritable> {
		/**
		 * @function Mapper ��������վ����
		 * @input key=ƫ����  value=����վ����
		 * @output key=weatherStationId value=temperature
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString(); //ÿ����������
			int temperature = Integer.parseInt(line.substring(14, 19).trim());//ÿСʱ����ֵ
			if (temperature != -9999) { //������Ч����				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);//ͨ���ļ�������ȡ����վid
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
		}
	}
	public static void main(String[] args) throws Exception {
		//��������·�������·��
		String[] args0 = {
							"hdfs://mini:9000/weather/",
							"hdfs://mini:9000/weather/out/"
						};
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		System.exit(ec);

	}
	/**
	 * 
	 * @function Reducer ͳ��������������վ��ƽ������
	 * @input key=weatherStationId  value=temperature
	 * @output key=weatherStationId value=average(temperature)
	 */
	public static class TemperatureReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable< IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			//ͳ��ÿ������վ������ֵ�ܺ�
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			//��ÿ������վ������ƽ��ֵ
			result.set(sum / count);
			context.write(key, result);
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();//��ȡ�����ļ�

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "temperature");//�½�һ������
		job.setJarByClass(Temperature.class);// ��������
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// ����·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// ���·��

		job.setMapperClass(TemperatureMapper.class);// Mapper
		job.setReducerClass(TemperatureReducer.class);// Reducer
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		return job.waitForCompletion(true)?0:1;//�ύ����
	}
}
