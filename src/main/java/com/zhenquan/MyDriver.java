package com.zhenquan;

import com.zhenquan.mapreduce.tv.TvCount;
import com.zhenquan.mapreduce.wordcount.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class MyDriver {
    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            pgd.addClass("tvcount", TvCount.class,
                    "A map/reduce program that counts the words in the input files.");
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
