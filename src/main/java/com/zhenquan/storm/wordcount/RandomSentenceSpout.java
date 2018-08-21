package com.zhenquan.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        String[] sentences = new String[] {
                "An empty street",
                "An empty house",
                "A hole inside my heart",
                "I'm all alone",
                "The rooms are getting smaller",
                "I wonder how",
                "I wonder why",
                "I wonder where they are",
                "The days we had",
                "The songs we sang together",
                "Oh yeah",
                "And oh my love",
                "I'm holding on forever",
                "Reaching for a love that seems so far",
                "So i say a little prayer",
                "And hope my dreams will take me there",
                "Where the skies are blue to see you once again	, my love",
                "Over seas and coast to coast",
                "To find a place i love the most",
                "Where the fields are green to see you once again ,	my love",
                "I try to read",
                "I go to work",
                "I'm laughing with my friends",
                "But i can't stop to keep myself from thinking",
                "Oh no I wonder how",
                "I wonder why",
                "I wonder where they are",
                "The days we had",
                "The songs we sang together",
                "Oh yeah And oh my love",
                "I'm holding on forever"
        };

        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
