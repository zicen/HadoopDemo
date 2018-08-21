package com.zhenquan.elasticsearch;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String b = "d221891124372920170227151128";
		String a=MD5Hash.getMD5AsHex(Bytes.toBytes("13911243722")).substring(0, 3);
		System.out.println(a);
	}

}
