//package com.zhenquan.elasticsearch;
//
//import java.util.List;
//
//import org.apache.commons.lang.StringUtils;
//
//import com.dajiangtai.djt_spider.entity.Page;
//import com.dajiangtai.djt_spider.util.ElasticSearchUtil;
//import com.dajiangtai.djt_spider.util.HbaseUtil;
//import com.dajiangtai.djt_spider.util.RedisUtil;
//import com.dajiangtai.djt_spider.util.ThreadUtil;
//
///**
// * ElasticSearch建立索引
// * Created by dajiangtai
// *
// */
//public class ESJob {
//	private static final String ES_TV_INDEX = "es_tv_index";
//	static RedisUtil redis = new RedisUtil();
//
//
//	public static void buildIndex(){
//		String tvId = "";
//		try {
//			System.out.println("开始建立索引！！！");
//			HbaseUtil hbaseUtil = new HbaseUtil();
//			tvId = redis.poll(ES_TV_INDEX);
//			while (!Thread.currentThread().isInterrupted()) {
//				if(StringUtils.isNotBlank(tvId)){
//					Page page = hbaseUtil.get(HbaseUtil.TABLE_NAME, tvId);
//					if(page !=null){
//						ElasticSearchUtil.addIndex("djt", "tv",page);
//					}
//					tvId = redis.poll(ES_TV_INDEX);
//				}else{
//					System.out.println("目前没有需要索引的数据，休息一会再处理！");
//					ThreadUtil.sleep(5000);
//				}
//			}
//		} catch (Exception e) {
//			redis.add(ES_TV_INDEX, tvId);
//			e.printStackTrace();
//		}
//	}
//
//	public static void main(String[] args) {
//		buildIndex();
//	}
//}
