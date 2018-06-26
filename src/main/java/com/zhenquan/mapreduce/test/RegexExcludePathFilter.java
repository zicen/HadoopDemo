package com.zhenquan.mapreduce.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter{

	@Override
	public boolean accept(Path path) {
		if (path.toString().contains(".svn")) {
			return false;
		}else {
			return true;
		}
		
	}

}
