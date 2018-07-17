package com.zhenquan.mapreduce.join2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinByCartesianProduct {
    public static class ReduceJoinByCartesianProductMapper extends Mapper<Object, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text combineValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String pathname = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (pathname.endsWith("records.txt")) {
                String[] arr = StringUtils.split(value.toString(), "\t");
                if (arr.length == 3) {
                    joinKey.set(arr[0]);
                    combineValue.set("records.txt" + arr[1] + "\t" + arr[2]);
                }

            } else if (pathname.endsWith("station.txt")) {
                String[] arr = StringUtils.split(value.toString(), "\t");
                if (arr.length == 2) {
                    joinKey.set(arr[0]);
                    combineValue.set("station.txt" + arr[1]);
                }
            }
            context.write(joinKey,combineValue);
        }
    }


    public static class ReduceJoinByCartesianProductReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftTable=new ArrayList<String>();
        private List<String> rightTable=new ArrayList<String>();
        private Text result=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            for (Text value:values
                 ) {
                String val = value.toString();
                if (val.startsWith("records.txt")) {
                    rightTable.add(val.replaceFirst("records.txt", ""));
                } else if (val.startsWith("station.txt")) {
                    leftTable.add(val.replaceFirst("station.txt", ""));
                }
            }

            //笛卡尔积
            for (String left:leftTable
                 ) {
                for (String right:rightTable
                     ) {
                    result.set(left + "\t" + right);
                    context.write(key, result);
                }
            }


        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = {"hdfs://mini:9000/record/records.txt"
                ,"hdfs://mini:9000/record/station.txt"
                ,"hdfs://mini:9000/record/out"
        };

        Path myPath = new Path(otherArgs[2]);
        FileSystem fileSystem = myPath.getFileSystem(configuration);
        Job job = new Job(configuration);
        job.setJarByClass(ReduceJoinByCartesianProduct.class);
        job.setMapperClass(ReduceJoinByCartesianProductMapper.class);
        job.setReducerClass(ReduceJoinByCartesianProductReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]) );
        FileInputFormat.addInputPath(job,new Path(otherArgs[1]) );
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
