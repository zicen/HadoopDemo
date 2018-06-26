package com.zhenquan.hdfs.copyfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

/**
 * 过滤文件
 */
public class RegexAcceptPathFilter implements PathFilter {
    private final String regex;

    public RegexAcceptPathFilter(String regex) {
        this.regex = regex;
    }

    /**
     * 如果要接收 regex 格式的文件，则accept()方法就return flag; 如果想要过滤掉regex格式的文件，则accept()方法就return !flag。
     *
     * @param path
     * @return
     */
    @Override
    public boolean accept(Path path) {
        boolean flag = path.toString().matches(regex);
        return flag;
    }


    public static void list(String srcPath, String dstPath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://mini:9000");
        FileSystem fs = FileSystem.get(configuration);
        LocalFileSystem local = FileSystem.getLocal(configuration);

        FileStatus[] excludeStatus = local.globStatus(new Path(srcPath), new RegexExcludePathFilter());
        Path[] excludePaths = FileUtil.stat2Paths(excludeStatus);
        for (Path path : excludePaths) {
            System.out.println(path.toString());
            String name = path.getName();
            String filename = name.replace("-", "");
            System.out.println("path name:" + name);
            System.out.println("filename:" + filename);
            FileStatus[] globStatus = local.globStatus(new Path(path.toString() + "/*"), new RegexAcceptPathFilter("^.*txt$"));
            Path[] stat2Paths = FileUtil.stat2Paths(globStatus);
            Path block = new Path(dstPath + filename + ".txt");
            FSDataOutputStream out = fs.create(block);
            if (stat2Paths != null) {
                for (Path p : stat2Paths) {
                    System.out.println(p.toString());
                    FSDataInputStream in = local.open(p);
                    IOUtils.copyBytes(in, out, 4096, false);
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            }

        }
    }

    public static void main(String[] args) throws IOException {
        list("E:/donwload/stock/*", "hdfs://mini:9000/filetest/");
    }

}
