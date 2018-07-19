package com.zhenquan.mapreduce.partition;

import org.apache.hadoop.io.WritableComparator;

/**
 * 组内排序策略
 */
public class TextIntComparator extends WritableComparator {
    public TextIntComparator() {
        super(TextInt.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        TextInt ti1 = (TextInt) a;
        TextInt ti2 = (TextInt) b;
        if (!ti1.getFirstKey().equals(ti2.getFirstKey())) {
            return ti1.getFirstKey().compareTo(ti2.getFirstKey());
        }else {
            return ti1.getSecondKey() - ti2.getSecondKey();
        }
    }
}
