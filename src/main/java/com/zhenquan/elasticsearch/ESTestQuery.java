package com.zhenquan.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import my.elasticsearch.action.search.SearchRequestBuilder;
import my.elasticsearch.action.search.SearchResponse;
import my.elasticsearch.client.transport.TransportClient;
import my.elasticsearch.common.settings.Settings;
import my.elasticsearch.common.transport.InetSocketTransportAddress;
import my.elasticsearch.index.query.QueryBuilders;
import my.elasticsearch.search.SearchHit;
import my.elasticsearch.search.SearchHits;
import my.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

/**
 * Query 操作
 * 
 * @author 大讲台
 * 
 */
public class ESTestQuery {
	private TransportClient client;

	@Before
	public void test0() throws UnknownHostException {

		// 开启client.transport.sniff功能，探测集群所有节点
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "escluster")
				.put("client.transport.sniff", true).build();
		// on startup
		// 获取TransportClient
		client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("mini"), 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("mini1"), 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("mini2"), 9300));
	}
	/**
	 * 查询：query
	 * 分页：from to
	 * 排序：sort
	 * 过滤：filter
	 * 
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		SearchRequestBuilder builder = client.prepareSearch("tv");
		builder.setTypes("tvcount");
		builder//.setQuery(QueryBuilders.matchQuery("tvname", "琅琊榜"))
//			   .setQuery(QueryBuilders.matchAllQuery())
			   .setQuery(QueryBuilders.multiMatchQuery("胡歌", "tvname","desciption","actor","director"))
			   //.setQuery(QueryBuilders.queryStringQuery("name:tom*"))
			   //.setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name", "tom").boost(3.0f)).should(QueryBuilders.matchQuery("age", 32).boost(1.0f)))
//				.setQuery(QueryBuilders.termQuery("_id", "yk_zd56886dc86fc11e3a705"))
				.setFrom(0)
				.setSize(10)
//				.addSort("age", SortOrder.ASC)
//				.setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(32))
				.setExplain(false)
				;
		SearchResponse searchResponse = builder.get();
		SearchHits hits = searchResponse.getHits();
		Map<String, Object> map = new HashMap<String, Object>();
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSource().toString());
		}
	}



}
