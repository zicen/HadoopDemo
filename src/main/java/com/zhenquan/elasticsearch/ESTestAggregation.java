package com.zhenquan.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import my.elasticsearch.action.bulk.BackoffPolicy;
import my.elasticsearch.action.bulk.BulkProcessor;
import my.elasticsearch.action.bulk.BulkRequest;
import my.elasticsearch.action.bulk.BulkResponse;
import my.elasticsearch.action.index.IndexRequest;
import my.elasticsearch.action.index.IndexResponse;
import my.elasticsearch.action.search.SearchRequestBuilder;
import my.elasticsearch.action.search.SearchResponse;
import my.elasticsearch.client.transport.TransportClient;
import my.elasticsearch.common.settings.Settings;
import my.elasticsearch.common.transport.InetSocketTransportAddress;
import my.elasticsearch.common.unit.ByteSizeUnit;
import my.elasticsearch.common.unit.ByteSizeValue;
import my.elasticsearch.common.unit.TimeValue;
import my.elasticsearch.index.query.QueryBuilders;
import my.elasticsearch.search.SearchHit;
import my.elasticsearch.search.SearchHits;
import my.elasticsearch.search.aggregations.AggregationBuilders;
import my.elasticsearch.search.aggregations.bucket.terms.Terms;
import my.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import my.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Before;
import org.junit.Test;
/**
 * Aggregation 操作
 *
 * 
 */
public class ESTestAggregation {
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
								.getByName("master"), 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("slave1"), 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("slave2"), 9300));
	}
	/**
	 * Aggregation 分组统计相同年龄学员个数
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		SearchRequestBuilder builder = client.prepareSearch("djt1");
		builder.setTypes("user")
			   .setQuery(QueryBuilders.matchAllQuery())
			   //按年龄分组聚合统计
			   .addAggregation(AggregationBuilders.terms("by_age").field("age").size(0))
				;
		
		SearchResponse searchResponse = builder.get();
		//获取分组信息
		Terms terms = searchResponse.getAggregations().get("by_age");
		List<Bucket> buckets = terms.getBuckets();
		for (Bucket bucket : buckets) {
			System.out.println(bucket.getKey()+":"+bucket.getDocCount());
		}
	}
	
	/**
	 * Aggregation 分组统计每个学员的总成绩
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		SearchRequestBuilder builder = client.prepareSearch("djt2");
		builder.setTypes("user")
			   .setQuery(QueryBuilders.matchAllQuery())
			   //按姓名分组聚合统计
			   .addAggregation(AggregationBuilders.terms("by_name")
					   .field("name")
			   .subAggregation(AggregationBuilders.sum("sum_score")
					   .field("score"))
			   .size(0))
				;		
		SearchResponse searchResponse = builder.get();
		//获取分组信息
		Terms terms = searchResponse.getAggregations().get("by_name");
		List<Bucket> buckets = terms.getBuckets();
		for (Bucket bucket : buckets) {
			Sum sum = bucket.getAggregations().get("sum_score");
			System.out.println(bucket.getKey()+":"+sum.getValue());
		}
	}
	
	/**
	 * 支持多索引和多类型查询
	 * @throws Exception
	 */
	@Test
	public void test3() throws Exception {
		SearchRequestBuilder builder 
		= client//.prepareSearch("djt1","djt2")//可以指定多个索引库
		.prepareSearch("djt*")//索引库可以使用通配符
		.setTypes("user");//支持多个类型，但不支持通配符

		SearchResponse searchResponse = builder.get();
		
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	/**
	 * 分片查询方式
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
		SearchRequestBuilder 
		builder = client.prepareSearch("djt3")
						.setTypes("user")
						//.setPreference("_local")
						//.setPreference("_only_local")
						//.setPreference("_primary")
						//.setPreference("_replica")
						//.setPreference("_primary_first")
						//.setPreference("_replica_first")
						//.setPreference("_only_node:Vfzj5oZCRi2blghSD3w0ZQ")
						//.setPreference("_prefer_node:nJL_MqcsSle6gY7iujoAlw")
						.setPreference("_shards:3")
						;

		SearchResponse searchResponse = builder.get();
		
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	/**
	 * 极速查询：通过路由插入数据（同一类别数据在一个分片）
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		Acount acount = new Acount("13602546655","tom1","male",16);
		Acount acount2 = new Acount("13602546655","tom2","male",17);
		Acount acount3 = new Acount("13602546655","tom3","male",18);
		Acount acount4 = new Acount("18903762536","john1","male",28);
		Acount acount5 = new Acount("18903762536","john2","male",29);
		Acount acount6 = new Acount("18903762536","john3","male",30);
		List<Acount> list = new ArrayList<Acount>();
		list.add(acount);
		list.add(acount2);
		list.add(acount3);
		list.add(acount4);
		list.add(acount5);
		list.add(acount6);
		
		BulkProcessor bulkProcessor = BulkProcessor.builder(
		        client,  
		        new BulkProcessor.Listener() {
					
					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
						System.out.println(request.numberOfActions());
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							Throwable failure) {
						// TODO Auto-generated method stub
						System.out.println(failure.getMessage());
					}
					
					public void afterBulk(long executionId, BulkRequest request,
							BulkResponse response) {
						// TODO Auto-generated method stub
						System.out.println(response.hasFailures());
					}
				})
		        .setBulkActions(1000) // 每个批次的最大数量
		        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))// 每个批次的最大字节数
		        .setFlushInterval(TimeValue.timeValueSeconds(5))// 每批提交时间间隔
		        .setConcurrentRequests(1) //设置多少个并发处理线程
		        //可以允许用户自定义当一个或者多个bulk请求失败后,该执行如何操作
		        .setBackoffPolicy(
		            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) 
		        .build();
		for (Acount a : list) {
			ObjectMapper mapper = new ObjectMapper();

			byte[] json = mapper.writeValueAsBytes(a);
			bulkProcessor.add(new IndexRequest("djt3", "user")
								.routing(a.getPhone().substring(0, 3))
								.source(json));
		}
		
		//阻塞至所有的请求线程处理完毕后，断开连接资源
				bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
				client.close();
	}
	/**
	 * 极速查询：通过路由极速查询，也可以通过分片shards查询演示
	 * 
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		SearchRequestBuilder builder = client.prepareSearch("djt3")//可以指定多个索引库
											 .setTypes("user");//支持多个类型，但不支持通配符
		builder.setQuery(QueryBuilders.matchAllQuery())
			   .setRouting("13602546655".substring(0, 3))
			   //.setRouting("18903762536".substring(0, 3))
			   ;
		SearchResponse searchResponse = builder.get();
		
		SearchHits hits = searchResponse.getHits();
		SearchHit[] hits2 = hits.getHits();
		for (SearchHit searchHit : hits2) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
}
