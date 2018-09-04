package com.zhenquan.storm.ack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

/**
 * Created by maoxiangyi on 2016/8/18.
 */
public class Bolt2 extends BaseRichBolt {
    private OutputCollector collector;

    //初始化方法 只调用一次
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    //被循环调用
    public void execute(Tuple input) {
        String string = input.getString(0);
        System.out.println("bolt2的execute input.getString(0):"+string);
        String uuid = (String) input.getValueByField("uuid");
        System.out.println("bolt2的uuid:"+uuid);
        collector.emit(input,new Values(input.getString(0)));
        System.out.println("bolt2的execute方法被调用一次" + input.getString(0));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uuid"));
    }
}
