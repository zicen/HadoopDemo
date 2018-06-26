package com.zhenquan.mapreduce.tv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TvCount extends Configured implements Tool{
	 public static class TvMapper extends Mapper< Text, TVWritable, Text, TVWritable > {
	        @Override
	        protected void map(Text key, TVWritable value, Context context)
	                throws IOException, InterruptedException {
	        	System.out.println("value:"+value.toString());
	        	
	        	context.write(key, value);
	        }
	    }
	    
	    public static class TvReducer extends Reducer< Text, TVWritable, Text, Text > {
	        private MultipleOutputs< Text, Text> multipleOutputs;
	       @Override
	    protected void setup(Context context)
	    		throws IOException, InterruptedException {
	    	   multipleOutputs =  new MultipleOutputs< Text, Text>(context);
	    }
	        
	        protected void reduce(Text key, Iterable< TVWritable > Values, Context context)
	                throws IOException, InterruptedException {
	        	long bofang = 0 ,shoucang = 0,pinglun=0,cai=0,zan=0;
	   
	        	for(TVWritable value:Values) {	
	        			bofang+=value.bofang;
		        		shoucang += value.shoucang;
		        		pinglun  += value.pinglun;
		        		cai += value.cai;
		        		zan += value.zan;
					
	        	}
	        	String[] records = key.toString().split("\t");
	        	
	        	Text result = new Text(bofang+" "+shoucang+" "+pinglun+" "+cai+" "+zan);
	        	if ("1".equals(records[1])) {
	        		multipleOutputs.write(key,result ,"youku");
				}else if ("2".equals(records[1])) {
					multipleOutputs.write(key,result ,"shouhu");
				}else if ("3".equals(records[1])) {
					multipleOutputs.write(key,result ,"tudou");
				}else if ("4".equals(records[1])) {
					multipleOutputs.write(key,result ,"aiqiyi");
				}else if ("5".equals(records[1])) {
					multipleOutputs.write(key,result ,"xunlei");
				}
	        	
	        
	        
	        }
	        
	        @Override
	        protected void cleanup(Context context)
	        		throws IOException, InterruptedException {
	        	multipleOutputs.close();
	        }
	    }
	@Override
	public int run(String[] args) throws Exception {
		 	Configuration conf = new Configuration();	        
	        Path mypath = new Path(args[1]);
	        FileSystem hdfs = mypath.getFileSystem(conf);
	        if (hdfs.isDirectory(mypath)) {
	            hdfs.delete(mypath, true);
	        }
	        
	        Job job = new Job(conf, "TvCount");
	        job.setJarByClass(TvCount.class);
	        
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        job.setMapperClass(TvMapper.class);// Mapper
	        job.setReducerClass(TvReducer.class);// Reducer
	        
	        job.setMapOutputKeyClass(Text.class);// Mapper key
	        job.setMapOutputValueClass(TVWritable.class);// Mapper value
	                
	        job.setInputFormatClass(ReadHotInputFormat.class);
	        
	        job.waitForCompletion(true);        
	        return 0;
	}
	public static void main(String[] args) throws Exception {
		 String[] args0 = { 
	        		"hdfs://mini:9000/tvplay/tvplay.txt",
	                "hdfs://mini:9000/tvplay/tvplay-out/" 
	                };
	        int ec = ToolRunner.run(new Configuration(), new TvCount(), args0);
	        System.exit(ec);
	}

}
