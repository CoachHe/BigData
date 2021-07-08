package fieldgrouping.bolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class WordNormalizer implements IRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    Random rand;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        Utils.sleep(10000);
        String[] sentences = new String[] {
                "this is a test",
                "hello world",
                "I am fine",
                "china duan"
        };
        String s = sentences[rand.nextInt(sentences.length)];

        System.out.println("in nextTuple():" + s);
        collector.emit(new Values(s));

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
