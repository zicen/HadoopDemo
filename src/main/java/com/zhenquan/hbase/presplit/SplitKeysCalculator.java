package com.zhenquan.hbase.presplit;

public interface SplitKeysCalculator {
	byte[][] calcSplitKeys();
}
