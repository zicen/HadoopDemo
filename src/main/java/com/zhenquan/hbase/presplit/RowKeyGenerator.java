package com.zhenquan.hbase.presplit;

public interface RowKeyGenerator {
    byte [] nextId();
}
