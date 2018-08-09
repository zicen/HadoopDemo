package com.zhenquan.hbase.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 读取 HBase 中的数据
 * 编写 Mapper 函数，读取 < Key，Value> 值，通过 Reducer 函数直接输出得到的结果就行了。
 */
public class MapReduceReaderHbaseDriver {

    public static class WordCountHbaseMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer("");
            //获取列族content下面所有的值
            for (java.util.Map.Entry<byte[], byte[]> value : values
                    .getFamilyMap("content".getBytes()).entrySet()) {
                String str = new String(value.getValue());
                if (str != null) {
                    sb.append(str);
                }
                context.write(new Text(key.get()), new Text(new String(sb)));
            }
        }
    }

    public static class WordCountHbaseReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values
                    ) {
                result.set(val);
                System.out.println("key:" + key.toString() + ",value:" + result.toString());
                context.write(key, result);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tableName = "wordcount";//hbase表名称
        Configuration conf = HBaseConfiguration.create(); //实例化 Configuration
        conf.set("hbase.zookeeper.quorum", "mini"); //hbase服务地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");//端口号
        Path path = new Path("hdfs://mini1:9000/test/out-reader");
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        Job job = new Job(conf, "import from hbase to hdfs");
        job.setJarByClass(MapReduceReaderHbaseDriver.class);

        job.setReducerClass(WordCountHbaseReducer.class);
        //设置读取hbase时的相关操作
        TableMapReduceUtil.initTableMapperJob(tableName, new Scan(), WordCountHbaseMapper.class, Text.class, Text.class, job, false);

        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

