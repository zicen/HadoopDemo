package com.zhenquan.elasticsearch.mysql2hbase;

import com.zhenquan.elasticsearch.JDBCUtil;
import org.junit.Test;

import java.util.List;

/**
 * 数据导入HBase并向ElasticSearch添加索引
 *
 */
public class ESDataImportAndIndex {
	public static void main(String[] args) throws Exception {
		String sql = "select * from tvcount";
		//1.查询mysql的数据
		List<TVCount> list = JDBCUtil.queryData(sql);
		HbaseUtil hbaseUtil = new HbaseUtil();
		for (int i=0;i<list.size();i++) {
			try {
				TVCount tv = list.get(i);
				String tvId = tv.getTvid();
				//2.把数据插入hbase
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_TVNAME, tv.getTvname());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_DIRECTOR, tv.getDirector());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_ACTOR, tv.getActor());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_ALLNUMBER, tv.getAllnumber());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_TVTYPE, tv.getTvtype());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_DESCRIPTION, tv.getDescription());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_ALIAS, tv.getAlias());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_TVSHOW, tv.getTvshow());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_PRESENT, tv.getPresent());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_SCORE, tv.getScore());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_ZONE, tv.getZone());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_COMMENTNUMBER, tv.getCommentnumber());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_SUPPORTNUMBER, tv.getSupportnumber());
				hbaseUtil.put(HbaseUtil.TABLE_NAME, tvId, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_PIC, tv.getPic());
				//3.把数据插入es
				ElasticSearchUtil.addIndex("tv", "tvcount", tv);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testElasticSearch(){
        int count = ElasticSearchUtil.getCount("琅琊榜", "tvcount", "tv");
        System.out.println("琅琊榜 count:"+count);



    }



}
