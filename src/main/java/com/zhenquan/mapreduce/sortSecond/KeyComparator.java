package com.zhenquan.mapreduce.sortSecond;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    public KeyComparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntPair ip1 = (IntPair) w1;
        IntPair ip2 = (IntPair) w2;
        int l = ip1.first;
        int r = ip2.first;

        if (l != r) {
            return l > r ? 1 : -1;
        } else if (ip1.second != ip2.second) {
            return ip1.second > ip2.second ? 1 : -1;
        } else {
            return 0;
        }
    }
}
