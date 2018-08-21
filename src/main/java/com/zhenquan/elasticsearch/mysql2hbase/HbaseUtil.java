package com.zhenquan.elasticsearch.mysql2hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * HBase 工具类 Created by dajiangtai on 2016-10-04
 */
@SuppressWarnings("unused")
public class HbaseUtil {

    /**
     * HBASE 表名称
     */
    public static final String TABLE_NAME = "tvcount";
    /**
     * 列簇1
     */
    public static final String COLUMNFAMILY_1 = "tvinfo";
    /**
     * 列簇1中的列
     */
    public static final String COLUMNFAMILY_1_TVNAME = "tvname";
    public static final String COLUMNFAMILY_1_DIRECTOR = "director";
    public static final String COLUMNFAMILY_1_ACTOR = "actor";
    public static final String COLUMNFAMILY_1_ALLNUMBER = "allnumber";
    public static final String COLUMNFAMILY_1_TVTYPE = "tvtype";
    public static final String COLUMNFAMILY_1_DESCRIPTION = "description";
    public static final String COLUMNFAMILY_1_TVID = "tvid";
    public static final String COLUMNFAMILY_1_ALIAS = "alias";
    public static final String COLUMNFAMILY_1_TVSHOW = "tvshow";
    public static final String COLUMNFAMILY_1_PRESENT = "present";
    public static final String COLUMNFAMILY_1_SCORE = "score";
    public static final String COLUMNFAMILY_1_ZONE = "zone";
    public static final String COLUMNFAMILY_1_COMMENTNUMBER = "commentnumber";
    public static final String COLUMNFAMILY_1_SUPPORTNUMBER = "supportnumber";
    public static final String COLUMNFAMILY_1_PIC = "pic";

    /**
     * 列簇2
     */
    public static final String COLUMNFAMILY_2 = "episode";

    /**
     * 列簇2中的列
     */
    public static final String COLUMNFAMILY_2_EPISODENUMBER = "episodenumber";


    HBaseAdmin admin = null;
    Configuration conf = null;

    /**
     * 构造函数加载配置
     */
    public HbaseUtil() {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.195.10:2181");
        conf.set("hbase.rootdir", "hdfs://192.168.195.10:9000/hbase");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        HbaseUtil hbase = new HbaseUtil();
        // 创建一张表
        hbase.createTable(TABLE_NAME, COLUMNFAMILY_1);
        // 查询所有表名
        hbase.getALLTable();
    }

    /**
     * rowFilter的使用
     *
     * @param tableName
     * @param reg
     * @throws Exception
     */
    public void getRowFilter(String tableName, String reg) throws Exception {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        // Filter
        RowFilter rowFilter = new RowFilter(CompareOp.NOT_EQUAL,
                new RegexStringComparator(reg));
        scan.setFilter(rowFilter);
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(new String(result.getRow()));
        }
    }

    // scan数据
    public void getScanData(String tableName, String family, String qualifier)
            throws Exception {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.addColumn(family.getBytes(), qualifier.getBytes());
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            if (result.raw().length == 0) {
                System.out.println(tableName + " 表数据为空！");
            } else {
                for (KeyValue kv : result.raw()) {
                    System.out.println(new String(kv.getKey()) + "\t"
                            + new String(kv.getValue()));
                }
            }
        }
    }

    private void deleteTable(String tableName) {
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName + "表删除成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(tableName + "表删除失败！");
        }

    }

    /**
     * 删除一条记录
     *
     * @param tableName
     * @param rowKey
     */
    public void deleteOneRecord(String tableName, String rowKey) {
        HTablePool hTablePool = new HTablePool(conf, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Delete delete = new Delete(rowKey.getBytes());
        try {
            table.delete(delete);
            System.out.println(rowKey + "记录删除成功！");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(rowKey + "记录删除失败！");
        }
    }

    /**
     * 获取表的所有数据
     *
     * @param tableName
     */
    public void getALLData(String tableName) {
        try {
            HTable hTable = new HTable(conf, tableName);
            Scan scan = new Scan();
            ResultScanner scanner = hTable.getScanner(scan);
            for (Result result : scanner) {
                if (result.raw().length == 0) {
                    System.out.println(tableName + " 表数据为空！");
                } else {
                    for (KeyValue kv : result.raw()) {
                        System.out.println(new String(kv.getKey()) + "\t"
                                + new String(kv.getValue()));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 添加一条记录
    public void put(String tableName, String row, String columnFamily,
                    String column, String data) throws IOException {
        HTablePool hTablePool = new HTablePool(conf, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Put p1 = new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));
        table.put(p1);
        System.out.println("put'" + row + "'," + columnFamily + ":" + column
                + "','" + data + "'");
    }

    /**
     * 查询所有表名
     *
     * @return
     * @throws Exception
     */
    public List<String> getALLTable() throws Exception {
        ArrayList<String> tables = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] listTables = admin.listTables();
            if (listTables.length > 0) {
                for (HTableDescriptor tableDesc : listTables) {
                    tables.add(tableDesc.getNameAsString());
                    System.out.println(tableDesc.getNameAsString());
                }
            }
        }
        return tables;
    }

    /**
     * 创建一张表
     *
     * @param tableName
     * @param column
     * @throws Exception
     */
    public void createTable(String tableName, String column) throws Exception {
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + "表已经存在！");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));
            admin.createTable(tableDesc);
            System.out.println(tableName + "表创建成功！");
        }
    }

    /**
     * 获取电视剧详细信息
     *
     * @param tableName
     * @return Created by dajiangtai on 2016-10-13
     */
    @SuppressWarnings({"deprecation", "resource"})
    public TVCount get(String tableName, String id) {
        HTablePool hTablePool = new HTablePool(conf, 1000);
        HTableInterface table = hTablePool.getTable(tableName);
        Get get = new Get(Bytes.toBytes(id));
        TVCount tv = null;
        try {
            Result result = table.get(get);
            KeyValue[] raw = result.raw();
            tv = new TVCount();
            tv.setTvid(id);
            tv.setActor(new String(raw[0].getValue(), "UTF-8"));
            tv.setAlias(new String(raw[1].getValue(), "UTF-8"));
            tv.setAllnumber(new String(raw[2].getValue(), "UTF-8"));
            tv.setCommentnumber(new String(raw[3].getValue(), "UTF-8"));
            tv.setDescription(new String(raw[4].getValue(), "UTF-8"));
            tv.setDirector(new String(raw[5].getValue(), "UTF-8"));
            tv.setPic(new String(raw[6].getValue(), "UTF-8"));
            tv.setPresent(new String(raw[7].getValue(), "UTF-8"));
            tv.setScore(new String(raw[8].getValue(), "UTF-8"));
            tv.setSupportnumber(new String(raw[9].getValue(), "UTF-8"));
            tv.setTvname(new String(raw[10].getValue(), "UTF-8"));
            tv.setTvshow(new String(raw[11].getValue(), "UTF-8"));
            tv.setTvtype(new String(raw[12].getValue(), "UTF-8"));
            tv.setZone(new String(raw[13].getValue(), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tv;
    }

    /**
     * 查看某一列的多个版本的值
     * <p>
     * 电视剧ID
     *
     * @param family    列簇
     * @param qualifier 列名
     * @param id
     * @return Created by dajiangtai on 2016-10-13
     */
    @SuppressWarnings({"resource", "deprecation"})
    public List<Map<String, String>> getCellMoreVersion(String tableName,
                                                        String family, String qualifier, String id) {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        try {
            HTablePool hTablePool = new HTablePool(conf, 1000);
            HTableInterface table = hTablePool.getTable(tableName);
            Get get = new Get(Bytes.toBytes(id));
            get.setMaxVersions();
            Result result = table.get(get);
            List<Cell> columnCells = result.getColumnCells(
                    Bytes.toBytes(family), Bytes.toBytes(qualifier));
            for (int i = 0; i < columnCells.size(); i++) {
                Cell cell = columnCells.get(i);
                long timestamp = cell.getTimestamp();
                Map<String, String> map = new HashMap<String, String>();
                map.put("time", DateUtil.formatDate(new Date(timestamp)));
                map.put("value", new String(cell.getValue()));
                list.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

}