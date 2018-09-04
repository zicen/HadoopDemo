package com.zhenquan.storm.itcastwordcount;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Map --->word,1
 */
public class MySplitBolt  extends BaseBasicBolt {



    public void execute(Tuple input, BasicOutputCollector collector) {
        //1、数据如何获取
        String juzi = (String)input.getValueByField("juzi");
        //2、进行切割
        String[] strings = juzi.split(" ");
        //3、发送数据
        for (String word:strings){
            //Values 对象帮我们生成一个list
             collector.emit(new Values(word,1));
        }
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}
