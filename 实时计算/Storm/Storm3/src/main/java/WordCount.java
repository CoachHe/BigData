import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WordCount {

    public static class WordProduct implements IRichSpout{
        private static final long serialVersionUID = 1L;
        private SpoutOutputCollector collector;
        Random rand;

        /**
         * open是第一个要执行的方法，也就是继承了IRichSpout所需要实现的第一个方法。用来初始化
         * @param map:创建Topology时的配置
         * @param topologyContext:所有Topology的数据
         * @param spoutOutputCollector:用来把Spout的数据发射给bolt
         */
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            System.out.println("in open()******");
            this.collector = collector;
            rand = new Random();
        }

        public void close() {

        }

        public void activate() {

        }

        public void deactivate() {

        }

        /**
         * 这是核心方法，会不断被调用，为了降低对CPU的消耗，当任务完成时sleep一下
         */
        public void nextTuple() {
            Utils.sleep(10000);
            String[] sentences = new String[]{
                    "this is a test",
                    "hello world",
                    "I am fine",
                    "china strong"
            };
            //随意取一个字符串
            String s = sentences[rand.nextInt(sentences.length)];

            System.out.println("in nextTuple(): " + s);
            collector.emit(new Values(s));
        }

        public void ack(Object o) {
            System.out.println("OK:" + o);
        }

        public void fail(Object o) {
            System.out.println("FAIL:" + o);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentences"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    //到这里就已经建立完一个spout了，举例topology还需要bolt

    public static class SplitSentence implements IRichBolt {
        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        /**
         * bolt中最重要的方法，每当接收到一个tuple时，此方法便被调用
         * 这个方法的作用就是把文本的每一行切分成一个个单词，并把这些单词发射出去，给下一个bolt处理器
         * @param tuple:多个tuple就组成stream
         */
        public void execute(Tuple tuple) {
            String s = tuple.getString(0);
            String[] word = s.split(" ");
            for (String w : word) {
                w = w.trim();
                if (w.length() != 0) {
                    collector.emit(new Values(w));
                }
            }
            //确定成功处理一个tuple
            collector.ack(tuple);
        }

        /**
         * Topology执行完毕之后的清理工作，比如关闭连接、释放资源等操作都会写在这里
         * 这里因为是简单展示就只打印
         */
        public void cleanup() {
            System.out.println("cleanup()");
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    //到这里一个bolt就处理完了，还需要一个bolt就能组成完整的topology了

    public static class WordCounter implements IRichBolt {
        private OutputCollector collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        /**
         * 作用是统计单词次数
         * @param tuple
         */
        public void execute(Tuple tuple) {
            String w = tuple.getString(0);
            Integer count = counts.get(w);
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            counts.put(w, count);
            collector.emit(new Values("word", count));
            System.out.println(w + ":" + Integer.toString(count));
            //确定成功处理一个tuple
            collector.ack(tuple);
        }


        /**
         * 思路和之前相同，打印计数器
         */
        public void cleanup() {
            System.out.println("cleanup()");
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));

        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        //定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();

        //为topology设置spout
        builder.setSpout("input", new WordProduct(), 1);

        //为topology设置bolt
        builder.setBolt("bold_sentence", new SplitSentence(), 1).shuffleGrouping("input");
        builder.setBolt("bold_wordcounter", new WordCounter(), 1).fieldsGrouping("bolt_sentence", new Fields("word"));

        //配置
        Config config = new Config();
        //Topology_debug(setDebug), 当它被设置成true的话，storm会记录下每个组件所发射的每条信息。这在本地环境调试topology很有用。但是线上这么做会影响性能
//        config.setDebug(true);
        //定义希望集群分配多少个工作进行给你来执行这个topology
        config.setNumWorkers(2);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            System.out.println("start word count");
            cluster.submitTopology("word count", config, builder.createTopology());
            //运行10秒
            Utils.sleep(10000);
            System.out.println("begin kill topology");
            cluster.killTopology("begin kill topology");
            cluster.shutdown();
        }
    }


}
