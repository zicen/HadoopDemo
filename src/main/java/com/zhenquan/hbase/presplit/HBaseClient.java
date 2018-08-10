package com.zhenquan.hbase.presplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * hbase 客户端
 *
 * @author dajiangtai
 */
public class HBaseClient {
    private static Log log = LogFactory.getLog(HBaseClient.class);
    private Connection connection;
    private static String zkString = "mini,mini1,mini2";
    //private static String zkString = "djt";
    private static String zkPort = "2181";

    //建表  create 'task','cf'

    //表名称
    public static final String TABLE_NAME = "tesk";

    //列簇
    public static final String COLUMNFAMILY = "cf";
    //列
    public static final String COLUMNFAMILY_UID = "uid";
    public static final String COLUMNFAMILY_SYSTASKID = "systaskid";
    public static final String COLUMNFAMILY_TASKID = "taskid";
    public static final String COLUMNFAMILY_NAME = "name";
    public static final String COLUMNFAMILY_TYPE = "type";
    public static final String COLUMNFAMILY_STATE = "state";
    public static final String COLUMNFAMILY_STARTTIME = "starttime";    //2018-02-03
    public static final String COLUMNFAMILY_FINISHTIME = "finishtime";
    public static final String COLUMNFAMILY_RECEIVETIME = "receivedate";
    public static final String COLUMNFAMILY_ACTUALFINISHTIME = "actualfinishtime";


    private HBaseClient(String zookeeperQuorum, String clientPort) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            log.error("create hbase connetion error!", e);
        }
    }

    /**
     * 获取HBaseClient对象
     *
     * @return
     */
    public static HBaseClient getHBaseClient() {
        HBaseClient hBaseClient = new HBaseClient(zkString, zkPort);
        return hBaseClient;
    }

    /**
     * 获取表对象
     *
     * @param tableName
     * @return
     */
    public Table getTable(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            table = null;
            log.error("get habse table error,tableName=" + tableName, e);
        }

        return table;
    }


    /**
     * 插入数据
     *
     * @param tableName
     * @param row
     * @param columnFaily
     * @param column
     * @param value
     * @throws IOException
     */
    public void put(String tableName, String row, String columnFaily, String column, String value) throws IOException {
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 统计表所有记录
     *
     * @param tableName
     * @throws IOException
     */
    public void queryAllTask(String tableName) throws IOException {
        Table table = getTable(tableName);
        Scan s = new Scan();
        ResultScanner rsa = table.getScanner(s);
        int num = 0;
        for (Result result : rsa) {
            String rowkey = new String(result.getRow());
            num++;
            System.out.println(num + "~" + rowkey);
        }
    }

    /**
     * 删除数据
     *
     * @param tableName
     * @param row
     * @throws IOException
     */
    public void deleteRecord(String tableName, String row) throws IOException {
        Table table = getTable(tableName);
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
    }

    /**
     * 关闭表连接
     *
     * @param table
     */
    public void closeTable(Table table) {
        if (table != null) {
            try {
                table.close();
            } catch (Exception e) {
                log.error("close table error,tableName=" + table.getName(), e);
            }
        }
    }

    /**
     * 关闭connection连接
     */
    public void close() {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.error("close hbase connect error", e);
            }
        }
    }


}
