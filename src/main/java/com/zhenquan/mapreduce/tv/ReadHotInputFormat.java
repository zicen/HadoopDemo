package com.zhenquan.mapreduce.tv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReadHotInputFormat extends FileInputFormat<Text, TVWritable>{
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public RecordReader<Text, TVWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new TVRecordReader();
	}
    public static class TVRecordReader extends RecordReader< Text, TVWritable > {
        public LineReader in;
        public Text lineKey;
        public TVWritable lineValue;
        public Text line;
        
        @Override
        public void close() throws IOException {
            if(in !=null){
                in.close();
            }
        }
        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return lineKey;
        }
        @Override
        public TVWritable getCurrentValue() throws IOException,
                InterruptedException {
            return lineValue;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }
        @Override
        public void initialize(InputSplit input, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit split=(FileSplit)input;
            Configuration job=context.getConfiguration();
            Path file=split.getPath();
            FileSystem fs=file.getFileSystem(job);
            
            FSDataInputStream filein=fs.open(file);
            in=new LineReader(filein,job);
            line=new Text();
            lineKey=new Text();
            lineValue = new TVWritable();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            int linesize=in.readLine(line);
            if(linesize==0) return false;
            String[] pieces = line.toString().split("\t");
            if(pieces.length != 7){
            	throw new IOException("Invalid record received");
            }
            
                long a,b,c,d,e;
                try{
         
                	a = Long.parseLong(pieces[2].trim());
                    b = Long.parseLong(pieces[3].trim());
                    c = Long.parseLong(pieces[4].trim());
                    d = Long.parseLong(pieces[5].trim());
                    e = Long.parseLong(pieces[6].trim());
                }catch(NumberFormatException nfe){
                    throw new IOException("Error parsing floating poing value in record");
                }
                lineKey.set(pieces[0]+"\t"+pieces[1]);
                lineValue.set(a,b, c, d, e);
                return true;
			
            
        }        
    }
}
