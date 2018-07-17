package com.zhenquan.mapreduce.sortSecond;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortSecond extends Configured implements Tool {
    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private IntPair keyLine = new IntPair();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int left = 0, right = 0;
            String[] split = line.split("\\s+");
            if (split.length == 2) {
                left = Integer.parseInt(split[0]);
                right = Integer.parseInt(split[1]);
                keyLine.set(left, right);
                context.write(keyLine, new IntWritable(right));
            }

//            StringTokenizer tokenizer = new StringTokenizer(line);
//            System.out.println("line:" + line + ",tokenizer:");
//
//            if (tokenizer.hasMoreTokens()) {
//                left = Integer.parseInt(tokenizer.nextToken());
//                if (tokenizer.hasMoreTokens()) {
//                    right = Integer.parseInt(tokenizer.nextToken());
//                    keyLine.set(left, right);
//                    context.write(keyLine, new IntWritable(right));
//                }
//            }
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, Text> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable right : values
                    ) {
                System.out.println(key.first+" "+right.toString());
                context.write(new Text(key.first + ""), new Text(right + ""));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path myPath = new Path(args[1]);

        Configuration conf = new Configuration();
        FileSystem fileSystem = myPath.getFileSystem(conf);
        if (fileSystem.isDirectory(myPath)) {
            fileSystem.delete(myPath, true);
        }
        Job job = new Job(conf, "secondarysort");
        job.setJarByClass(SortSecond.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));//输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        job.setMapperClass(Map.class);// Mapper
        job.setReducerClass(Reduce.class);// Reducer

        job.setPartitionerClass(FirstPartitioner.class);// 分区函数
        job.setSortComparatorClass(KeyComparator.class);//本课程并没有自定义SortComparator，而是使用IntPair自带的排序
        job.setGroupingComparatorClass(GroupingComparator.class);// 分组函数


//这里不使用MapOutputKey就出错了
//        job.setMapOutputKeyClass(IntPair.class);
//        job.setMapOutputValueClass(IntWritable.class);


//可加可不加
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {"hdfs://mini:9000/sort/sort.txt", "hdfs://mini:9000/sort/out/"};
        int run = ToolRunner.run(new Configuration(), new SortSecond(), args0);
        System.exit(run);
    }
}
