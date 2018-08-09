package com.zhenquan.hbase;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Before;

/**
 * 模拟插入hbase数据
 *
 * @author 大讲台
 */
public class Test2 {

    HBaseAdmin admin = null;
    Configuration conf = null;

    @Before
    public void test0() throws UnknownHostException {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @org.junit.Test
    public void testHashAndCreateTable() throws Exception {
        TableName tableName = TableName.valueOf("task5");
        HTable table = new HTable(conf, tableName);
        int uid = 11772;
        for (int i = 0; i <= 100; i++) {
            byte[] rowkey = Bytes.add(MD5Hash.getMD5AsHex((uid + "").getBytes()).substring(0, 8).getBytes(), Bytes.toBytes(uid + "20180526" + "3256"));
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("uid"), Bytes.toBytes(uid));
            table.put(put);
            uid++;
        }
    }

}
