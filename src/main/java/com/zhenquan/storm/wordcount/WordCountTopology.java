package com.zhenquan.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "wordcount-topology";

    public static void main(String[] args) throws InterruptedException {
        // 构造一个RandomSentenceSpout对象
        RandomSentenceSpout sentenceSpout = new RandomSentenceSpout();
        // 构造一个SplitSentenceBlot对象
        SplitSentenceBlot splitBolt = new SplitSentenceBlot();
        // 构造一个WordCountBolt对象
        WordCountBolt countBolt = new WordCountBolt();
        // 构造一个ReportBolt对象
        ReportBolt reportBolt = new ReportBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout, 5);
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 8).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt, 12).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }


}
