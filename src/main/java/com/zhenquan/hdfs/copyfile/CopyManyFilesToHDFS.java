package com.zhenquan.hdfs.copyfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *  将指定格式的多个文件上传至 HDFS
 */
public class CopyManyFilesToHDFS {
    private static FileSystem fs = null;
    private static FileSystem local = null;

    /**
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     * @function Main 方法
     */
    public static void main(String[] args) throws IOException, URISyntaxException {
        //文件上传路径
        Path dstPath = new Path("hdfs://mini:9000/middle/filter/");
        //调用文件上传 list 方法
        list(dstPath);
    }

    public static void list(Path dstPath) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://mini:9000");
        //获取文件系统对象
        fs = FileSystem.get(uri, conf);
        //获取本地文件系统
        local = FileSystem.getLocal(conf);
        //*****使用globStatus(Path pathPattern, PathFilter filter)，完成文件格式过滤，获取所有 txt 格式的文件。*****
        FileStatus[] localStatus = local.globStatus(new Path("E:/donwload/stock/*"), new RegexAcceptPathFilter("^.*txt$"));
        //获得所有文件路径
        Path[] listedPaths = FileUtil.stat2Paths(localStatus);
        //*****然后使用 Java API 接口 copyFromLocalFile，将所有 txt 格式的文件上传至 HDFS。*****
        for (Path p : listedPaths
                ) {
            fs.copyFromLocalFile(p, dstPath);
        }
    }
}
