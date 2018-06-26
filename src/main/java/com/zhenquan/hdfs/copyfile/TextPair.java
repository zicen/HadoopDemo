package com.zhenquan.hdfs.copyfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	private Text first;
	private Text second;
	public TextPair() {
		this.set(new Text(),new Text());
	}
	public TextPair(Text text, Text text2) {
		this.set(text,text2);
	}
	public TextPair(String text,String text2) {
		this.set(new Text(text),new Text(text2));
	}
	private void set(Text text, Text text2) {
		this.first = text;
		this.second = text2;
	}
	public Text getFirst() {
		return this.first;
	}
	public Text getSecond() {
		return this.second;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		first.write(arg0);
		second.write(arg0);
		
	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode() * 163 + second.hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair tPair = (TextPair) obj;
			return first.equals(tPair.first) && second.equals(tPair.second);
		}
		return false;
	}
	@Override
	public int compareTo(TextPair tp) {		
			int cmp = first.compareTo(tp.first);
			if (cmp!=0) {
				return cmp;
			}
			return second.compareTo(tp.second);		
		
	}
	
}
