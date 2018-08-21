package com.zhenquan.storm.wordcount;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 4921144902730095910L;
    private HashMap<String,Integer> counts = null;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counts = new HashMap();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        this.counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("-------print counts for my love------");
        List<String> keys = new ArrayList();
        keys.addAll(counts.keySet());
        Collections.sort(keys);
        for (String key:keys
             ) {
            System.out.println(key+":"+this.counts.get(key));
        }
    }
}
