package com.zhenquan.mapreduce.join2;

import com.zhenquan.hdfs.copyfile.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    protected GroupingComparator(){
        super(TextPair.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
       TextPair ip1 = (TextPair) a;
       TextPair ip2 = (TextPair) b;
        Text l = ip1.getFirst();
        Text r = ip2.getFirst();
        return l.compareTo(r);
    }
}
