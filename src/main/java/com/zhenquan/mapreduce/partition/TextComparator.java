package com.zhenquan.mapreduce.partition;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator {
    public TextComparator() {
        super(TextInt.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextInt ti1 = (TextInt) a;
        TextInt ti2 = (TextInt) b;
        return ti1.getFirstKey().compareTo(ti2.getFirstKey());
    }
}
