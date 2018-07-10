package com.zhenquan.mapreduce.sortSecond;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    public int first;
    public int second;

    @Override
    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first > o.first ? 1 : -1;
        } else if (second != o.second) {
            return second > o.second ? 1 : -1;
        } else {
            return 0;
        }
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }
}
