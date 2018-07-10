package com.zhenquan.mapreduce.sortSecond;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    public GroupingComparator() {
        super(IntPair.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        IntPair ip1 = (IntPair) w1;
        IntPair ip2 = (IntPair) w2;
        int l = ip1.first;
        int r = ip2.second;
        if (l == r) {
            if (ip1.second == ip2.second) {
                return 0;
            }
            return ip1.second > ip2.second ? 1 : -1;
        }else {
            return l > r ? 1 : -1;
        }

    }
}
