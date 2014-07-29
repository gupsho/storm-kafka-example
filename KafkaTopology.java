package com.example.storm;

import backtype.storm.LocalCluster;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

import com.example.storm.KafkaSpout;
import com.example.storm.PrinterBolt;


public class KafkaTopology {
	
	private static final String TOPIC_NAME = "topic-12";
	
	public static void main(String[] args) throws Exception {
        
        KafkaSpout kafkaSpout = new KafkaSpout(TOPIC_NAME);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout, 2);
        builder.setBolt("printer", new PrinterBolt(), 2).shuffleGrouping("spout");
        
        Config conf = new Config();
        conf.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka", conf, builder.createTopology());
        
        Thread.sleep(10000);
        cluster.killTopology("kafka");

        cluster.shutdown();
        
	}
}
