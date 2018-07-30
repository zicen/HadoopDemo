package com.zhenquan.hbase;

import com.zhenquan.mapreduce.tv.TvCount;
import com.zhenquan.mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ProgramDriver;

import java.io.IOException;

public class HbaseDemo {
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "mini,mini1,mini2");
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conf.set("hbase.master", "mini:60000");
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        hTableDescriptor.addFamily(new HColumnDescriptor("address"));//添加Family
        hTableDescriptor.addFamily(new HColumnDescriptor("info")); //添加Family

        hBaseAdmin.createTable(hTableDescriptor);//创建表
        hBaseAdmin.close();
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void insertDataByput(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        //创建一个对象也就是一个行
        Put put = new Put(getBytes("zhenquan"));
        put.add(getBytes("address"), getBytes("country"), getBytes("china"));
        put.add(getBytes("address"), getBytes("province"), getBytes("上海"));
        put.add(getBytes("address"), getBytes("city"), getBytes("shanghai"));

        put.add(getBytes("info"), getBytes("age"), getBytes("20"));
        put.add(getBytes("info"), getBytes("birthday"), getBytes("1988-12-12"));
        put.add(getBytes("info"), getBytes("company"), getBytes("ronghang"));

        table.put(put);
        table.close();

    }

    /**
     * 查询
     *
     * @param tableName
     * @throws IOException
     */
    public static void QueryByGet(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get zhenquan = new Get(getBytes("zhenquan"));
        Result result = table.get(zhenquan);
        System.out.println("获取到rowkey:" + new String(result.getRow()));
        for (KeyValue keyvalue : result.raw()
                ) {
            System.out.println("列簇：" + new String(keyvalue.getFamily())
                    + "====列" + new String(keyvalue.getQualifier()) + "====值" + new String(keyvalue.getValue()));
        }
        table.close();

    }

    /**
     * scan查询
     *
     * @param tableName
     * @throws IOException
     */
    public static void QueryByScan(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.addColumn(getBytes("info"), getBytes("company"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner
                ) {
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyvalue : r.raw()
                    ) {
                System.out.println("列簇：" + new String(keyvalue.getFamily())
                        + "====列" + new String(keyvalue.getQualifier()) + "====值" + new String(keyvalue.getValue()));
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 删除数据
     * @param tableName
     * @throws IOException
     */
    public static void deleteData(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Delete delete = new Delete(getBytes("zhenquan"));
        delete.deleteColumn(getBytes("info"), getBytes("age"));
        table.delete(delete);
        table.close();
    }
    public static byte[] getBytes(String string) {
        if (string == null)
            string = "";
        return Bytes.toBytes(string);
    }

    public static void main(String[] args) throws IOException {
//        createTable("member");
//        insertDataByput("member");
//        QueryByGet("member");
//        QueryByScan("member");
//        deleteData("member");
    }
}
