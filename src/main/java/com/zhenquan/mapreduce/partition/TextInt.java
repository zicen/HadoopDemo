package com.zhenquan.mapreduce.partition;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextInt implements WritableComparable {
    //直接利用java的基本数据类型
    private String firstKey;
    private int secondKey;
    //必须要有一个默认的构造函数
    public String getFirstKey() {
        return firstKey;
    }
    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }
    public int getSecondKey() {
        return secondKey;
    }
    public void setSecondKey(int secondKey) {
        this.secondKey = secondKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(firstKey);
        out.writeInt(secondKey);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        firstKey=in.readUTF();
        secondKey=in.readInt();
    }
    //map的键的比较就是根据这个方法来进行的
    @Override
    public int compareTo(Object o) {
        // TODO Auto-generated method stub
        TextInt ti=(TextInt)o;
        //利用这个来控制升序或降序
        //this本对象写在前面代表是升序
        //this本对象写在后面代表是降序
        return this.getFirstKey().compareTo(ti.getFirstKey());
    }
}
