package com.zhenquan.hbase;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 预建分区
 * @author dajiangtai
 *
 */
public class TestCreateRegion {
	HBaseAdmin admin = null;
	Configuration conf = null;
	@Before
	public void test0() throws UnknownHostException {
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		//conf.set("hbase.zookeeper.quorum", "djt");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testHashAndCreateTable() throws Exception{
		 	HashChoreWoker worker = new HashChoreWoker(1000000,10);
	        byte [][] splitKeys = worker.calcSplitKeys();
	        TableName tableName = TableName.valueOf("task5");
	        
	        if (admin.tableExists(tableName)) {
	            try {
	                admin.disableTable(tableName);
	            } catch (Exception e) {
	            }
	            admin.deleteTable(tableName);
	        }

	        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
	        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("cf"));
	        columnDesc.setMaxVersions(1);
	        tableDesc.addFamily(columnDesc);

	        admin.createTable(tableDesc ,splitKeys);

	        admin.close();
	    }
}
