package com.zhenquan.mapreduce.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import sun.reflect.generics.tree.Tree;

public class GroupingComparator extends WritableComparator{
	protected  GroupingComparator() {
		super(TextPair.class,true);
	}
	
	 //Compare two WritableComparables.
    public int compare(WritableComparable w1, WritableComparable w2){
        TextPair ip1 = (TextPair) w1;
        TextPair ip2 = (TextPair) w2;
        Text l = ip1.getFirst();
        Text r = ip2.getFirst();
        return l.compareTo(r);
    }
}
