package com.zhenquan.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 通过协处理器的observer实现二级索引
 */
public class InverIndexCoprocessor extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        //这就是我们要插入的二级索引表  （要提前创建好）
        Table ffindex = connection.getTable(TableName.valueOf("ffindex"));
        byte[] row = put.getRow();
        //获取rowkey
        String rowKey = new String(row);
        //获取cell 列的value
        Cell cell = put.get("fi".getBytes(), "address".getBytes()).get(0);
        byte[] valueArray = cell.getValueArray();
        String address = new String(valueArray, cell.getValueOffset(), cell.getValueLength());
        String[] user_fensi = rowKey.split("-");
        //倒过来插入
        Put putindex = new Put((user_fensi[1] + "-" + user_fensi[0]).getBytes());
        putindex.addColumn("fi".getBytes(), "address".getBytes(), address.getBytes());
        ffindex.put(putindex);
        ffindex.close();
    }
}
