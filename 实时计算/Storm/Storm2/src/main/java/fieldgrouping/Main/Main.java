package fieldgrouping.Main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.coachhe.WordCount;
import fieldgrouping.spout.WordReader;

public class Main {

    public static void main(String[] args) {

        //定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();

        //为topology设置spout(节点)
        builder.setSpout("input", new WordCount.WordProduct(),1);

        //配置
        Config config = new Config();
        //TOPOLOGY_DEBUG(setDebug), 当它被设置成true的话， storm会记录下每个组件所发射的每条消息。这在本地环境调试topology很有用， 但是在线上这么做的话会影响性能的。
        //config.setDebug(true);
        //定义你希望集群分配多少个工作进程给你来执行这个topology.
        config.setNumWorkers(2);

        if(args!=null && args.length>0) {
            config.setNumWorkers(3);
            //StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            //创建一个本地模式cluster
            LocalCluster cluster = new LocalCluster();
            System.out.println("start word count");
            cluster.submitTopology("word count", config, builder.createTopology());
            //运行60s
            Utils.sleep(10000);

            System.out.println("begin kill topology");
            cluster.killTopology("word count"); //终止topology任务
            cluster.shutdown();
        }

    }



}
