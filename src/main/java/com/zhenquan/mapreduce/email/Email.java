package com.zhenquan.mapreduce.email;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Email extends Configured implements Tool {
    public static class EmailMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));
        }
    }

    public static class EmailReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            System.out.println("reduce key:" + key.toString());
            int begin = key.toString().indexOf("@");
            int end = key.toString().indexOf(".");
            if (begin > end) {
                return;
            }

//            multipleOutputs.write(key, new IntWritable(1), key.toString().substring(begin + 1, end));

            String[] split = key.toString().split("@");
            if (split.length == 2) {
                String[] split1 = split[1].split("\\.");
                if (split1.length == 2) {
                    multipleOutputs.write(key, new Text(""), split1[0]);
                }

            }


        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Path myPath = new Path(args[1]);
        FileSystem hdfs = myPath.getFileSystem(configuration);
        if (hdfs.isDirectory(myPath)) {
            hdfs.delete(myPath, true);
        }

        Job job = new Job();
        job.setJarByClass(Email.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, myPath);
        job.setMapperClass(EmailMapper.class);
        job.setReducerClass(EmailReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {"hdfs://mini:9000/email/email.txt", "hdfs://mini:9000/email/out"};
        int run = ToolRunner.run(new Configuration(), new Email(), args0);
        System.exit(run);
    }
}
