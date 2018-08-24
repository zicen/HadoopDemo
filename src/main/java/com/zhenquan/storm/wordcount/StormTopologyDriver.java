package com.zhenquan.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by maoxiangyi on 2016/8/16.
 */
public class StormTopologyDriver {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MyLocalFileSpout(),2);
        topologyBuilder.setBolt("bolt1", new MySplitBolt(),4).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("bolt2", new MyWordCountAndPrintBolt(),2).shuffleGrouping("bolt1");

        //2、任务提交
        //提交给谁？提交什么内容？
        Config config = new Config();
        config.setNumWorkers(2);
        StormTopology stormTopology = topologyBuilder.createTopology();
//        //本地模式
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("wordcount", config, stormTopology);
        //集群模式
        StormSubmitter.submitTopology("wordcount1", config, stormTopology);
    }
}
