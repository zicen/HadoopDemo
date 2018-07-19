package com.zhenquan.mapreduce.join;

import com.zhenquan.mapreduce.partition.KeyPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoinBySecondarySort extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();// 读取配置文件
        Path mypath = new Path(args[2]);
        FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        Job job = Job.getInstance(conf, "join");// 新建一个任务
        job.setJarByClass(ReduceJoinBySecondarySort.class);// 主类

        Path recordInputPath = new Path(args[0]);//天气记录数据源
        Path stationInputPath = new Path(args[1]);//气象站数据源
        Path outputPath = new Path(args[2]);//输出路径

        MultipleInputs.addInputPath(job, recordInputPath, TextInputFormat.class, JoinRecordMapper.class);//读取天气记录Mapper
        MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);//读取气象站Mapper

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setReducerClass(JoinReducer.class);// Reducer
        job.setNumReduceTasks(2);
        job.setPartitionerClass(KeyPartition.class);//自定义分区
        job.setGroupingComparatorClass(GroupingComparator.class);//自定义分组

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        System.out.println(job.getNumReduceTasks());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args0) throws Exception {
        String[] args = {"hdfs://mini:9000/record/records.txt"
                , "hdfs://mini:9000/record/station.txt"
                , "hdfs://mini:9000/record/out"
        };
        int exitCode = ToolRunner.run(new ReduceJoinBySecondarySort(), args);
        System.exit(exitCode);
    }

}
