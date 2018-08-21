package com.zhenquan.elasticsearch.mysql2hbase;

import org.apache.commons.lang.StringUtils;
import my.elasticsearch.action.index.IndexResponse;
import my.elasticsearch.action.search.SearchRequestBuilder;
import my.elasticsearch.action.search.SearchResponse;
import my.elasticsearch.action.search.SearchType;
import my.elasticsearch.client.Client;
import my.elasticsearch.client.transport.TransportClient;
import my.elasticsearch.common.settings.Settings;
import my.elasticsearch.common.text.Text;
import my.elasticsearch.common.transport.InetSocketTransportAddress;
import my.elasticsearch.index.query.QueryBuilders;
import my.elasticsearch.search.SearchHit;
import my.elasticsearch.search.SearchHits;
import my.elasticsearch.search.highlight.HighlightField;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch 全文索引工具类
 * 
 * @author dajiangtai
 * 
 */
public class ElasticSearchUtil {
	public static Client client = null;

	/**
	 * 获取客户端
	 * 
	 * @return
	 */
	public static Client getClient() {
		if (client != null) {
			return client;
		}
		Settings settings = Settings.settingsBuilder()
				//集群名称
				.put("cluster.name", "escluster")
				.put("client.transport.sniff", true).build();
		try {
			client = TransportClient
					.builder()
					.settings(settings)
					.build()
					//设置ElasticSearch集群名称+端口号
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName("mini"), 9300))
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName("mini1"), 9300))
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName("mini2"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;
	}

	/**
	 * 添加索引
	 * 
	 * @param index
	 * @param type
	 * @param page
	 * @return
	 */
	public static String addIndex(String index, String type, TVCount page) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		//hashMap.put("tvId", page.getTvId());
		hashMap.put("tvname", page.getTvname());
		hashMap.put("director", page.getDirector());
		hashMap.put("description", page.getDescription());
		hashMap.put("score", page.getScore());
		hashMap.put("actor", page.getActor());

		IndexResponse response = getClient().prepareIndex(index, type)
											.setId(page.getTvid())//设置id
											.setSource(hashMap)
											.execute()
											.actionGet();
		System.out.println("success"+response.getId());
		return response.getId();
	}
	/**
	 * 查询总记录数
	 * @return
	 */
	public static int getCount(String key , String type , String index) {
		SearchRequestBuilder builder = getClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if (StringUtils.isNotBlank(key)) {
			builder.setQuery(QueryBuilders.termQuery("tvname", key));
		}
		builder.setExplain(true);
		SearchResponse searchResponse = builder.get();

		SearchHits hits = searchResponse.getHits();
		int total = (int) hits.getTotalHits();
		return total;
	}
	
	/**
	 * es 数据查询
	 * @param key
	 * @param index
	 * @param type
	 * @param start
	 * @param row
	 * @return
	 */
	public static List<Page> search(String key, String index,
			String type, int start, int row) {
		SearchRequestBuilder builder = getClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setFrom(start);
		builder.setSize(row);
		// 设置高亮字段名称
		builder.addHighlightedField("tvname");
		// 设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		// 设置高亮后缀
		builder.setHighlighterPostTags("</font>");
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if (StringUtils.isNotBlank(key)) {
			builder.setQuery(QueryBuilders.matchQuery("tvname", key));
		}
		builder.setExplain(true);
		SearchResponse searchResponse = builder.get();

		SearchHits hits = searchResponse.getHits();
		Map<String, Object> map = new HashMap<String, Object>();
		SearchHit[] hits2 = hits.getHits();
		List<Page> list = new ArrayList<Page>();
		for (SearchHit searchHit : hits2) {
			Map<String, HighlightField> highlightFields = searchHit
					.getHighlightFields();
			
			Map<String, Object> source = searchHit.getSource();
			Page page = new Page();
			page.setTvId(searchHit.getId());
			page.setTvname(source.get("tvname").toString());
//			page.setAllnumber(source.get("allnumber").toString());
//			page.setDaynumber(source.get("daynumber").toString());
//			page.setCollectnumber(source.get("collectnumber").toString());
//			page.setCommentnumber(source.get("commentnumber").toString());
//			page.setSupportnumber(source.get("supportnumber").toString());
//			page.setAgainstnumber(source.get("againstnumber").toString());
			HighlightField highlightField = highlightFields.get("tvname");
			if (highlightField != null) {
				Text[] fragments = highlightField.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				page.setTvname(name);
			}

			list.add(page);
		}
		return list;
	}
	
	/**
	 * es TVCount数据查询
	 * @param key
	 * @param index
	 * @param type
	 * @param start
	 * @param row
	 * @return
	 */
	public static List<TVCount> searchForTVCount(String key, String index,
			String type, int start, int row) {
		SearchRequestBuilder builder = getClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setFrom(start);
		builder.setSize(row);
		// 设置高亮字段名称
		builder.addHighlightedField("tvname");
		builder.addHighlightedField("director");
		builder.addHighlightedField("actor");
		builder.addHighlightedField("tvtype");
		builder.addHighlightedField("description");
		// 设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		// 设置高亮后缀
		builder.setHighlighterPostTags("</font>");
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if (StringUtils.isNotBlank(key)) {
			builder.setQuery(QueryBuilders.multiMatchQuery( key,"tvname","director","actor","tvtype","description"));
		}
		builder.setExplain(true);
		SearchResponse searchResponse = builder.get();

		SearchHits hits = searchResponse.getHits();
		Map<String, Object> map = new HashMap<String, Object>();
		SearchHit[] hits2 = hits.getHits();
		List<TVCount> list = new ArrayList<TVCount>();
		for (SearchHit searchHit : hits2) {
			Map<String, HighlightField> highlightFields = searchHit
					.getHighlightFields();
			
			Map<String, Object> source = searchHit.getSource();
			
			TVCount tv = new TVCount();
			tv.setTvname(source.get("tvname").toString());
			tv.setDirector(source.get("director").toString());
			tv.setActor(source.get("actor").toString());
			tv.setDescription(source.get("description").toString());
			tv.setTvtype(source.get("tvtype").toString());
			tv.setPic(source.get("pic").toString());
			tv.setAllnumber(source.get("allnumber").toString());
			tv.setTvid(searchHit.getId());
			
			HighlightField highlightField_tvname = highlightFields.get("tvname");
			if (highlightField_tvname != null) {
				Text[] fragments = highlightField_tvname.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				tv.setTvname(name);
			}
			HighlightField highlightField_director = highlightFields.get("director");
			if (highlightField_director != null) {
				Text[] fragments = highlightField_director.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				tv.setDirector(name);
			}
			
			HighlightField highlightField_actor = highlightFields.get("actor");
			if (highlightField_actor != null) {
				Text[] fragments = highlightField_actor.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				tv.setActor(name);
			}
			
			HighlightField highlightField_tvtype = highlightFields.get("tvtype");
			if (highlightField_tvtype != null) {
				Text[] fragments = highlightField_tvtype.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				tv.setTvtype(name);
			}
			
			HighlightField highlightField_description = highlightFields.get("description");
			if (highlightField_description != null) {
				Text[] fragments = highlightField_description.fragments();
				String name = "";
				for (Text text : fragments) {
					name += text;
				}
				tv.setDescription(name);
			}

			list.add(tv);
		}
		return list;
	}
	
	public static void addTestData(){
		Page page = new Page();
		page.setTvId("youku_zd56886dc23fc11e3a639");
		page.setTvname("麻雀");
		page.setAllnumber("1");
		page.setDaynumber("1");
		page.setCommentnumber("1");
		page.setCollectnumber("1");
		page.setSupportnumber("1");
		page.setAgainstnumber("1");
//		ElasticSearchUtil.addIndex("djt", "tv", page);
	}

	 public static void main(String[] args) {
		 //ElasticSearchUtil.addTestData();
		 
		 List<Page> list = ElasticSearchUtil.search("琅琊榜", "tv", "tvcount", 0,
	 10);
	      for(Page page : list){
	    	  System.out.println(page.getTvname());
	      }
	 }
}
