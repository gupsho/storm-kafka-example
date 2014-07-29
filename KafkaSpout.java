package com.example.storm;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class KafkaSpout extends BaseRichSpout {

	private SimpleConsumer _consumer;
	private static String _clienId = "client";
	private String _topic;
	SpoutOutputCollector _collector;
	
	public KafkaSpout(String topic) {
		this._topic = topic;
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		long offset = 0L;
        FetchRequest req = new FetchRequestBuilder()
        .clientId(KafkaSpout._clienId)
        .addFetch(this._topic, 0, offset, 100)
        .build();
        
        FetchResponse fetchResponse = this._consumer.fetch(req);
        try {
			this._collector.emit(new Values(getMessage((ByteBufferMessageSet) fetchResponse.messageSet(this._topic, 0))));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this._consumer  = new SimpleConsumer("localhost", 9092, 10000, 1024000, KafkaSpout._clienId); 
		
		this._collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));

	}
	
	private static String getMessage(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
		byte[] bytes = new byte[2];
		for(MessageAndOffset messageAndOffset: messageSet) {
			ByteBuffer payload = messageAndOffset.message().payload();
		bytes = new byte[payload.limit()];
		payload.get(bytes);
		
		}
		return new String(bytes, "UTF-8");
	}

}
