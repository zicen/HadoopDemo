package com.zhenquan.mapreduce.partition;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortMain extends Configured implements Tool {
    public static class GroupMapper extends Mapper<LongWritable, Text, TextInt, IntWritable> {
        public IntWritable second = new IntWritable();
        public TextInt tx = new TextInt();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = StringUtils.split(value.toString(), "\t");
            if (arr.length != 2) {
                return;
            }
            String lineKey = arr[0];
            String lineValue = arr[1];
            System.out.println("lineKey:" + lineKey + ",lineValue:" + lineValue);
            int lineInt = Integer.parseInt(lineValue);
            tx.setFirstKey(lineKey);
            tx.setSecondKey(lineInt);
            second.set(lineInt);
            context.write(tx, second);
        }
    }


    public static class GroupReduce extends Reducer<TextInt, IntWritable, Text, Text> {
        @Override
        protected void reduce(TextInt key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuffer str = new StringBuffer();
            for (IntWritable val : values
                    ) {
                str.append(val + ",");
            }
            if (str.length() > 0) {
                str.deleteCharAt(str.length() - 1);
            }
            context.write(new Text(key.getFirstKey()), new Text(str.toString()));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "SecondarySort");
        job.setJarByClass(SortMain.class);
        // 设置输入文件的路径，已经上传在HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出文件的路径，输出文件也存在HDFS中，但是输出目录不能已经存在
        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReduce.class);
        //设置分区方法
        job.setPartitionerClass(KeyPartition.class);

        //下面这两个都是针对map端的
        //设置分组的策略，哪些key可以放置到一组中
        job.setGroupingComparatorClass(TextComparator.class);
        //设置key如何进行排序在传递给reducer之前.
        //这里就可以设置对组内如何排序的方法
        /*************关键点**********/
        job.setSortComparatorClass(TextIntComparator.class);
        //设置输入文件格式
        job.setInputFormatClass(TextInputFormat.class);
        //使用默认的输出格式即TextInputFormat
        //设置map的输出key和value类型
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce的输出key和value类型
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        int exitCode = job.isSuccessful() ? 0 : 1;
        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {"hdfs://mini:9000/record/partition.txt"
                , "hdfs://mini:9000/record/partition-out"
        };
        int exitCode = ToolRunner.run(new SortMain(), args0);
        System.exit(exitCode);
    }
}
