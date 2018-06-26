package com.zhenquan.mapreduce.tv;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

public class TVWritable implements WritableComparable< Object >{
	public long bofang;
	public long shoucang;
	public long pinglun;
	public long cai;
	public long zan;
	public TVWritable(){     
	}
	public TVWritable(long bofang,long shoucang,long pinglun,long cai,long zan) {
	
		this.bofang = bofang;
		this.shoucang = shoucang;
		this.pinglun = pinglun;
		this.cai = cai;
		this.zan = zan;
	}
	public void set(long bofang,long shoucang,long pinglun,long cai,long zan){
	
		this.bofang = bofang;
		this.shoucang = shoucang;
		this.pinglun = pinglun;
		this.cai = cai;
		this.zan = zan;
	}
    @Override
    public void readFields(DataInput in) throws IOException {
    	
    	bofang = in.readLong();
    	shoucang = in.readLong();
    	pinglun = in.readLong();
    	cai = in.readLong();
    	zan = in.readLong();
    }
    @Override
    public void write(DataOutput out) throws IOException {
    
        out.writeLong(bofang);
        out.writeLong(shoucang);
        out.writeLong(pinglun);
        out.writeLong(cai);
        out.writeLong(zan);
    }
    @Override
    public int compareTo(Object o) {
        return 0;
    }
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return "bofang:"+bofang+",shoucang:"+shoucang+",pinglun:"+pinglun+",cai:"+cai+",zan:"+zan;
    }
	
}
