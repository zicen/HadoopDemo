package com.zhenquan.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

/**
 * ● 使用场景：一张表十分小、一张表很大。

 ● 用法:

 在提交作业的时候先将小表文件放到该作业的DistributedCache中，然后从DistributeCache中取出该小表进行join (比如放到Hash Map等等容器中)。然后扫描大表，
 看大表中的每条记录的join key /value值是否能够在内存中找到相同join key的记录，如果有则直接输出结果。

 DistributedCache是分布式缓存的一种实现，它在整个MapReduce框架中起着相当重要的作用，
 他可以支撑我们写一些相当复杂高效的分布式程序。说回到这里，JobTracker在作业启动之前会获取到DistributedCache的资源uri列表，
 并将对应的文件分发到各个涉及到该作业的任务的TaskTracker上。
 另外，关于DistributedCache和作业的关系，比如权限、存储路径区分、public和private等属性。
 */
public class MapJoinByDistributedCache extends Configured implements Tool {
    public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Hashtable<String, String> table = new Hashtable<String, String>();

        /**
         * 获取分布式缓存文件
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Path[] localCacheFiles = context.getLocalCacheFiles();
            if (localCacheFiles.length == 0) {
                throw new FileNotFoundException("Distributed cache file not found");
            }
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());
            FSDataInputStream in = null;
            in = fs.open(new Path(localCacheFiles[0].toString()));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String infoAddr = null;
            while (null != (infoAddr = br.readLine())) {//按行读取并解析气象站数据
                String[] records = infoAddr.split("\t");
                table.put(records[0], records[1]);//key为stationid,value为stationname
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueItems = StringUtils.split(value.toString(), "\t");
            String stationName = table.get(valueItems[0]);
            if (stationName != null) {
                context.write(new Text(stationName), value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();

        Path out = new Path(args[2]);
        FileSystem hdfs = out.getFileSystem(conf);// 创建输出路径
        if (hdfs.isDirectory(out)) {
            hdfs.delete(out, true);
        }
        Job job = Job.getInstance();//获取一个job实例
        job.setJarByClass(MapJoinByDistributedCache.class);
        FileInputFormat.addInputPath(job,
                new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(
                args[2]));
        //添加分布式缓存文件 station.txt
        job.addCacheFile(new URI(args[1]));
        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);// 输出key类型
        job.setOutputValueClass(Text.class);// 输出value类型
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        String[] args0 = {"hdfs://mini:9000/record/records.txt"
                ,"hdfs://mini:9000/record/station.txt"
                ,"hdfs://mini:9000/record/mapcache-out"
        };
        int ec = ToolRunner.run(new Configuration(),
                new MapJoinByDistributedCache(), args0);
        System.exit(ec);
    }
}
