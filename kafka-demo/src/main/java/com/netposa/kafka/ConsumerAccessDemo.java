package com.netposa.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerAccessDemo {
	
	
	public static void main(String[] args) throws Exception {
		
		
		//定义topic
		String topic="test";
		//组装消费者参数
		Properties prop=new Properties();
		prop.load(ConsumerAccessDemo.class.getClassLoader().getResourceAsStream("access_consumer.properties"));
		//创建消费者连接对象
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
		
		//此map中的key 指的是消费的topic的名称 value 指的是使用消费者的个数
		Map<String, Integer> topicCountMap=new HashMap<String, Integer>();
		topicCountMap.put(topic, 2);
		//topicCountMap.put("kafka", 1);
		
		//此map中的key 指的是消费的topic的名称 value 指的是该 topic返回的流的集合list 此list的个数和上一个topicCountMap的value一致。
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
		
		//KafkaStream<byte[], byte[]>kafa的消息流对象  泛型的key 指返回的消息的key的类型 ：value 指返回的消息的value[message]的类型 
		List<KafkaStream<byte[], byte[]>> list = messageStreams.get(topic);
		
		
		for (KafkaStream<byte[], byte[]> kafkaStream : list) {
			System.out.println(list.size());
			new Thread(new Worker(kafkaStream)).start();	
		}
	}
	
	static class Worker implements Runnable{

		//kafkaStream 通过构造函数传到此类里
		private KafkaStream<byte[], byte[]> kafkaStream;
		
		
		@Override
		public void run() {
			
			
			//消费者迭代器
			ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
			
			//此方法底层实现是读取一个阻塞的队列 channel: BlockingQueue  所以要使用多个线程进行分别处理
			while (iterator.hasNext()) {
				//MessageAndMetadata 消息本身  泛型的key 指返回的消息的key的类型 ：value 指返回的消息的value[message]的类型 
				MessageAndMetadata<byte[], byte[]> next = iterator.next();
				System.out.println(String.format("key:%s message:%s partition:%s offset:%s",
						next.key()==null?"":new String(next.key())
						,next.message()==null?"":new String(next.message())
						,next.partition(),next.offset()));
			}
			
		}


		public Worker(KafkaStream<byte[], byte[]> kafkaStream) {
			this.kafkaStream = kafkaStream;
		}
		
		
	}
	
	

}
