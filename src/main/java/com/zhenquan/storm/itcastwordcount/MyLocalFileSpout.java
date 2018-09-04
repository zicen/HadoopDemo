package com.zhenquan.storm.itcastwordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

/**
 * Created by maoxiangyi on 2016/8/16.
 */
public class MyLocalFileSpout extends BaseRichSpout {
    private  SpoutOutputCollector collector;
    private BufferedReader bufferedReader;
    private Map<Values, String> tupleMap = new HashMap<>();
    //初始化方法
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.bufferedReader = new BufferedReader(new FileReader(new File("C:\\Users\\ry\\Desktop\\wordcount.txt")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    //Storm实时计算的特性就是对数据一条一条的处理
    //while(true){
    // this.nextTuple()
    // }
    public void nextTuple() {
        //每被调用一次就会发送一条数据出去
        try {
            String line = bufferedReader.readLine();
            if (StringUtils.isNotBlank(line)){
                Values tuple = new Values(line);

                String messagesId = UUID.randomUUID().toString().replace("-","");
                collector.emit(tuple);
                tupleMap.put(tuple, messagesId);
                collector.emit(tuple, messagesId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        String message = tupleMap.get(msgId);
        collector.emit(new Values(message), msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("juzi"));
    }
}
