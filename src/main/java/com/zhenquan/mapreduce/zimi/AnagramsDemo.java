package com.zhenquan.mapreduce.zimi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

/**
 * 字谜游戏
 * 一本英文书籍包含成千上万个单词或者短语，现在我们需要在大量的单词中，找出相同字母组成的所有anagrams(字谜)。
 * 部分结果集如下：
 * cehors	cosher,chores,ochres,ochers
 * cehorst	troches,hectors,torches
 * cehort	troche,hector
 * cehortu	toucher,couther,retouch
 */
public class AnagramsDemo extends Configured implements Tool {


    private static String jobName = "temperature";
    private static String inputPath = "hdfs://mini:9000/anagram/";
    private static String outputPath = "hdfs://mini:9000/anagram/out/";

    public static class AnagramsMappper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String word = value.toString();
            System.out.println("word:" + word);
            String sortedWord = getSortedWord(word);
            System.out.println("sortedWord:" + sortedWord);
            context.write(new Text(sortedWord), value);
        }

        public String getSortedWord(String word) {
            ArrayList<String> letterList = new ArrayList<>();
            for (int i = 0; i < word.length(); i++) {
                letterList.add(word.substring(i, i + 1));
            }
            Collections.sort(letterList);
            StringBuffer sortedWord = new StringBuffer();
            for (int i = 0; i < letterList.size(); i++) {
                sortedWord.append(letterList.get(i));
            }
            return sortedWord.toString();
        }
    }

    public static class AnagramsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> valuesList = new ArrayList<>();
            for (Text text : values) {
                valuesList.add(text);
            }
            StringBuffer result = new StringBuffer();
            for (int i = 0; i < valuesList.size(); i++) {
                Text text = valuesList.get(i);
                if (valuesList.size() <= 1) {
                    result.append(text.toString());
                } else {
                    result.append(text.toString() + ",");
                }
            }

            System.out.println(result.toString());
            if (!"".equals(result.toString())) {
                context.write(key, new Text(result.toString()));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {
                inputPath,
                outputPath
        };
        int ec = ToolRunner.run(new Configuration(), new AnagramsDemo(), args0);
        System.exit(ec);

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        Job job = new Job(conf, jobName);
        job.setJarByClass(AnagramsDemo.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AnagramsMappper.class);
        job.setReducerClass(AnagramsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
