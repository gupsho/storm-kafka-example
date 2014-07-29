package com.example.storm;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	
	private static final String TOPIC_NAME = "topic-12";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.setProperty("metadata.broker.list",  "localhost:9092");
	    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
	    props.setProperty("request.required.acks", "1");
	    
	    System.out.println("Topic name is " + TOPIC_NAME);
	    ProducerConfig producerConfig = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(producerConfig);
		for(int i = 0; i < 10000; i++) {
			String message = "message-" + i;
			KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(TOPIC_NAME, message);
			producer.send(keyedMessage);
		}
		producer.close();
		
	}

}
