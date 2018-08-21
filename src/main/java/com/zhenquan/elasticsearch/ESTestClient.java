package com.zhenquan.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;


import my.elasticsearch.client.transport.TransportClient;
import my.elasticsearch.cluster.node.DiscoveryNode;
import my.elasticsearch.common.settings.Settings;
import my.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Before;
import org.junit.Test;
/**
 * 获取TransportClient
 * @author 大讲台
 *
 */
public class ESTestClient {

	/**
	 * 测试使用Java API 连接ElasticSearch 集群
	 * 
	 * @throws UnknownHostException
	 */
	@Test
	public void test1() throws UnknownHostException {
		// on startup
		// 获取TransportClient
		TransportClient client = TransportClient
				.builder()
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("mini"), 9300));
		
		List<DiscoveryNode> connectedNodes = client.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println("集群节点："+discoveryNode.getHostName());
		}
		// on shutdown
		client.close();
	}

	/**
	 * 生成环境下，ElasticSearch集群名称非默认需要显示设置
	 * 
	 * @throws UnknownHostException
	 */
	@Test
	public void test2() throws UnknownHostException {
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "escluster").build();
		// on startup
		// 获取TransportClient
		TransportClient client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("mini"), 9300));
		
		List<DiscoveryNode> connectedNodes = client.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println("集群节点："+discoveryNode.getHostName());
		}

		// on shutdown
		client.close();
	}

	/**
	 * 1、启动应用程序之后，client.transport.sniff能保证即使master挂掉也能连接上集群
	 * 2、master节点挂机同时应用程序重启，则无法连接ElasticSearch集群
	 * 
	 * @throws UnknownHostException
	 */
	@Test
	public void test3() throws UnknownHostException {

		// 开启client.transport.sniff功能，探测集群所有节点
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "escluster")
				.put("client.transport.sniff", true).build();
		// on startup
		// 获取TransportClient
		TransportClient client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("192.168.195.10"), 9300));

		List<DiscoveryNode> connectedNodes = client.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println("集群节点："+discoveryNode.getHostName());
		}

		// on shutdown
		client.close();
	}

	/**
	 * 1、启动应用程序之后，client.transport.sniff能保证即使master挂掉也能连接上集群
	 * 2、设置多节点，防止其中一个节点挂机同时应用程序重启，无法连接ElasticSearch集群问题
	 * 
	 * @throws UnknownHostException
	 */
	@Test
	public void test4() throws UnknownHostException {

		// 开启client.transport.sniff功能，探测集群所有节点
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "escluster")
				.put("client.transport.sniff", true).build();
		// on startup
		// 获取TransportClient
		TransportClient client = TransportClient
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
		
		List<DiscoveryNode> connectedNodes = client.connectedNodes();
		for (DiscoveryNode discoveryNode : connectedNodes) {
			System.out.println("集群节点："+discoveryNode.getHostName());
		}

		// on shutdown
		client.close();
	}
}
