package com.zhenquan.hdfs.copyfile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter{
	/**
	 * 不要SVN格式的数据
	 * @param path
	 * @return
	 */
	@Override
	public boolean accept(Path path) {
		if (path.toString().contains(".svn")) {
			return false;
		}else {
			return true;
		}
		
	}

}
