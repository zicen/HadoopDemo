package com.zhenquan.storm.itcastwordcount;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by maoxiangyi on 2016/8/16.
 */
public class StormTopologyDriver {
    public static void main(String[] args) {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MyLocalFileSpout(),2);
        topologyBuilder.setBolt("bolt1", new MySplitBolt(),4).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("bolt2", new MyWordCountAndPrintBolt(),2).fieldsGrouping("bolt1",new Fields("word"));

        //2、任务提交
        //提交给谁？提交什么内容？
        Config config = new Config();
        config.setNumWorkers(2);
        //去掉消息的可靠性
//        config.setNumAckers(0);
        StormTopology stormTopology = topologyBuilder.createTopology();
        //本地模式
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount", config, stormTopology);
        //集群模式
//        StormSubmitter.submitTopology("wordcount1", config, stormTopology);
    }
}
