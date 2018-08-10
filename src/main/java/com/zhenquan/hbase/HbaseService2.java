package com.zhenquan.hbase;

import com.zhenquan.hbase.utils.DateUtils;
import com.zhenquan.hbase.utils.MyStringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

public class HbaseService2 {
    //一个rowkey=MD5HASH+uid+time+taskid
    private HBaseClient hBaseClient;
    public String TABLE_NAME = "tesk";

    @Before
    public void test0() throws UnknownHostException {
        hBaseClient = HBaseClient.getHBaseClient();
    }

    @Test
    public void getTaskByRowKey() {
        //740c6fae11772201805263256
        byte[] rowkey = Bytes.add(MD5Hash.getMD5AsHex((11772 + "").getBytes()).substring(0, 8).getBytes(), Bytes.toBytes(11772 + "20180526" + "3256"));
        Table table = hBaseClient.getTable("tesk");
        Get get = new Get(rowkey);
        try {
            Result result = table.get(get);
            if (!result.isEmpty()) {
                for (KeyValue keyvalue : result.raw()
                        ) {
                    //获取列簇，列，还有列的值
                    System.out.println("列簇：" + new String(keyvalue.getFamily())
                            + ",key:" + new String(keyvalue.getQualifier()) + ",value:" + new String(keyvalue.getValue()));
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (table != null) {
                hBaseClient.closeTable(table);
            }
            hBaseClient.close();
        }
    }

    @Test
    public void getTaskByUid() {
        int uid = 11772;
        Table table = hBaseClient.getTable(TABLE_NAME);
        String fixedLengthStr = MyStringUtil.getFixedLengthStr(uid + "", 7);
        String startKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedLengthStr)).substring(0, 8) + fixedLengthStr;
        String fixedLengthStr2 = MyStringUtil.getFixedLengthStr(uid + 1 + "", 7);
        String endKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedLengthStr)).substring(0, 8) + fixedLengthStr2;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));

        try {
            ResultScanner results = table.getScanner(scan);
            for (Result result : results
                    ) {
                for (KeyValue keyvalue : result.raw()
                        ) {
                    //获取列簇，列，还有列的值
                    System.out.println("列簇：" + new String(keyvalue.getFamily())
                            + ",key:" + new String(keyvalue.getQualifier()) + ",value:" + new String(keyvalue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    public void getTaskByUidAndTime() {
        int uid = 11706;
        String startTime = "2016-06-16 11:17:27";
        Table table = hBaseClient.getTable(TABLE_NAME);
        String fixedLengthStr = MyStringUtil.getFixedLengthStr(uid + "", 7);
        byte[] startKey = Bytes.add(MD5Hash.getMD5AsHex((uid + "").getBytes()).substring(0, 8).getBytes(), Bytes.toBytes(uid + DateUtils.getDateFormatFromDay(DateUtils.YMD, startTime,
                DateUtils.YYYYMMDD)));
        byte[] endKey = Bytes.add(MD5Hash.getMD5AsHex((uid + "").getBytes()).substring(0, 8).getBytes(), Bytes.toBytes(uid + DateUtils.getDateBeforeOrAfter(DateUtils.YMD, startTime, 1,
                DateUtils.YYYYMMDD)));
        Scan s = new Scan();
        s.setStartRow(startKey);
        s.setStopRow(endKey);
        try {
            ResultScanner scanner = table.getScanner(s);
            for (Result result : scanner
                    ) {
                for (KeyValue keyvalue : result.raw()
                        ) {
                    //获取列簇，列，还有列的值
                    System.out.println("列簇：" + new String(keyvalue.getFamily())
                            + ",key:" + new String(keyvalue.getQualifier()) + ",value:" + new String(keyvalue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
