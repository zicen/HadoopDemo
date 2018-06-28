package com.zhenquan.mapreduce.score;

import java.io.IOException;
import java.nio.file.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class ScoreInputFormat extends FileInputFormat<Text, ScoreWritable>{
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public RecordReader<Text, ScoreWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new ScoreRecordReader();
	}
	public static class ScoreRecordReader extends RecordReader<Text, ScoreWritable>{
		public LineReader in; //行读取器
		public Text lineKey; //自定义key类型
		public ScoreWritable lineValueScoreWritable; //自定义value类型
		public Text lineText; //每行数据类型
		

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			if(in!=null){
				in.close();
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}

		@Override
		public ScoreWritable getCurrentValue() throws IOException,
				InterruptedException {
			return lineValueScoreWritable;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context)
				throws IOException, InterruptedException {
			//应该是固定代码，就是一个初始化的作用
			FileSplit split = (FileSplit)input;
			Configuration configuration =context.getConfiguration();
			Path file = split.getPath();
			org.apache.hadoop.fs.FileSystem fs = file.getFileSystem(configuration);
			FSDataInputStream fsInputStream = fs.open(file);
			in = new LineReader(fsInputStream, configuration);
			
			lineKey = new Text();
			lineValueScoreWritable = new ScoreWritable();
			lineText = new Text();
		}
		/**
		 * 我们只需根据自己的需求，重点编写nextKeyValue()方法即可，其它的方法比较固定，仿造着编码就可以了。
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int lineSize = in.readLine(lineText);
			if (lineSize==0) {
				return false;
			}
			String[] pieceStrings = lineText.toString().split("\\s+");//解析每行的数据
			if (pieceStrings.length !=7) {
				throw new IOException("Invalid record received");
			}
			float a,b,c,d,e;
			try {
				a= Float.parseFloat(pieceStrings[2].trim());
				b= Float.parseFloat(pieceStrings[3].trim());
				c= Float.parseFloat(pieceStrings[4].trim());
				d= Float.parseFloat(pieceStrings[5].trim());
				e= Float.parseFloat(pieceStrings[6].trim());
			} catch (NumberFormatException e2) {
				throw new IOException("Error parsing floating poing value in recordd");
			}
			lineKey.set(pieceStrings[0]+"\t"+pieceStrings[1]);
			lineValueScoreWritable.set(a, b, c, d, e);
			return true;
		}
		
	}
}
