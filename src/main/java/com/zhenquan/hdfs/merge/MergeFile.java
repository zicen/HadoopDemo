package com.zhenquan.hdfs.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class MergeFile {
    private static FileSystem fs = null;
    private static FileSystem local = null;

    public static void main(String[] args) throws IOException, URISyntaxException {
        list("E:\\donwload\\73\\73\\*", "hdfs://mini1:9000/merge");
    }

    private static void list(String srcPath, String dstPath) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://mini1:9000"), configuration);
        local = FileSystem.getLocal(configuration);
        FileStatus[] fileStatuses = local.globStatus(new Path(srcPath));
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path dir : paths
                ) {
            FileStatus[] fileStatuses1 = local.globStatus(new Path(dir + "/*"));
            Path[] paths1 = FileUtil.stat2Paths(fileStatuses1);

            FSDataOutputStream fsDataOutputStream = fs.create(new Path(dstPath + "/" + dir.getName() + ".txt"));

            for (Path p : paths1
                    ) {
                System.out.println(p.toString());
                FSDataInputStream open = local.open(p);

                IOUtils.copyBytes(open, fsDataOutputStream, 4096, false);
                open.close();
            }
            if (fsDataOutputStream != null) {
                fsDataOutputStream.close();
            }

        }

    }
}
