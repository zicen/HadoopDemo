package com.zhenquan.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import com.zhenquan.mapreduce.wordcount.WordCount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * 一个大表，一个小表
 * map 阶段：Semi Join解决小表整个记录内存放不下的场景，过滤大表
 * reduce 阶段：reduce side join
 */
public class SemiJoin {
	 private static Logger logger = Logger.getLogger(SemiJoin.class);  
    public static class SemiJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    	// 定义Set集合保存小表中的key
    			private Set<String> joinKeys = new HashSet<String>();
    			private Text joinKey = new Text();
    			private Text combineValue = new Text();

    			/**
    			 * 获取分布式缓存文件
    			 */
    			protected void setup(Context context) throws IOException,
    					InterruptedException {
    				BufferedReader br;
    				String infoAddr = null;
    				// 返回缓存文件路径
    				Path[] cacheFilesPaths = context.getLocalCacheFiles();
    				for (Path path : cacheFilesPaths) {
    					String pathStr = path.toString();
    					br = new BufferedReader(new FileReader(pathStr));
    					while (null != (infoAddr = br.readLine())) {
    						// 按行读取并解析气象站数据
    						String[] records = StringUtils.split(infoAddr.toString(),
    								"\t");
    						if (null != records) {
    							// key为stationID
    							joinKeys.add(records[0]);
    							logger.info("set up record[0]:"+records[0]);
    						}
    							
    					}
    				}
    			}



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            System.out.println("mapper value:"+value);
            if (pathName.endsWith("records-semi.txt")) {
                String[] valueItems = StringUtils.split(value.toString(), "\t");
                if (valueItems.length != 3) {
                    return;
                }
                if (joinKeys.contains(valueItems[0])) {
                    joinKey.set(valueItems[0]);
                    combineValue.set("records-semi.txt" + valueItems[1] + "\t" + valueItems[2]);
                    logger.info("map joinKey:"+joinKey.toString()+",value:"+combineValue.toString());
                    context.write(joinKey, combineValue);
                }
            } else if (pathName.endsWith("station.txt")) {
                String[] valueItems = StringUtils.split(value.toString(), "\t");
                if (valueItems.length != 2) {
                    return;
                }
                joinKey.set(valueItems[0]);
                combineValue.set("station.txt" + valueItems[1]);
                logger.info("map joinKey:"+joinKey.toString()+",value:"+combineValue.toString());
                context.write(joinKey, combineValue);
            }
        }
    }

    /*
     * reduce 端做笛卡尔积
     */
    public static class SemiJoinReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftTable = new ArrayList<String>();
        private List<String> rightTable = new ArrayList<String>();
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            for (Text value : values
                    ) {
                String s = value.toString();
                System.out.println("value:" + s);
                if (s.startsWith("station.txt")) {
                    leftTable.add(s.replaceFirst("station.txt", ""));
                } else if (s.startsWith("records-semi.txt")) {
                    rightTable.add(s.replaceFirst("records-semi.txt", ""));
                }
            }

            for (String left : leftTable
                    ) {
                for (String right : rightTable
                        ) {
                    result.set(left + "\t" + right);
                    System.out.println("reduce key:"+key.toString()+",value:"+result.toString());
                    logger.info("reduce key:"+key.toString()+",value:"+result.toString());
                    context.write(key, result);
                }

            }
        }
    }

    public static void main(String[] otherArgs) throws Exception {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//                .getRemainingArgs();
//        String[] otherArgs = {"hdfs://mini:9000/record/station.txt"
//        		,"hdfs://mini:9000/record/station.txt"
//                ,"hdfs://mini:9000/record/records-semi.txt"
//                ,"hdfs://mini:9000/record/semi-out"
//        };
        if (otherArgs.length < 2) {
            System.err.println("Usage: semijoin <in> [<in>...] <out>");
            System.exit(2);
        }

        //输出路径
        Path mypath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        Job job = Job.getInstance(conf, "SemiJoin");

        //添加缓存文件
        job.addCacheFile(new Path(otherArgs[0]).toUri());
        job.setJarByClass(SemiJoin.class);
        job.setMapperClass(SemiJoinMapper.class);
        job.setReducerClass(SemiJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //添加输入路径
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //添加输出路径
        FileOutputFormat.setOutputPath(job, new Path(
                otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

