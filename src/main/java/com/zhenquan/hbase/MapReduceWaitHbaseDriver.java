package com.zhenquan.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReduceWaitHbaseDriver {
    public static class WordCountMapperHbase extends Mapper<Object, Text, ImmutableBytesWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                System.out.println("mapper key:" + word.toString() + ",value:" + one.toString());
                context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())), one);
            }
        }
    }

    public static class WordCountReducerHbase extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        @SuppressWarnings("deprecation")
        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("reduce");
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(key.get());//put 实例化  key代表主键，每个单词存一行
            //三个参数分别为  列簇为content，列修饰符为count，列值为词频
            put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            System.out.println("key:" + key.toString() + ",value:" + sum);
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "wordcount";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "mini");
        conf.set("hbase.zookeeper.property.clientport", "2181");
//        conf.set("hbase.master", "mini:60000");

        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor content = new HColumnDescriptor("content");
        hTableDescriptor.addFamily(content);
        hBaseAdmin.createTable(hTableDescriptor);

        Job job = new Job(conf, "import from hdfs to hbase");
        job.setJarByClass(MapReduceWaitHbaseDriver.class);
        job.setMapperClass(WordCountMapperHbase.class);
        //设置插入hbase时的相关操作
        TableMapReduceUtil.initTableReducerJob(tableName, WordCountReducerHbase.class, job, null, null, null, null, false);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPaths(job, "hdfs://mini1:9000/test/test.txt");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
