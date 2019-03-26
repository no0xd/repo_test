package com.netposa.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {

	public static void main(String[] args) throws Exception {
		
		//定义topic
		String topic="test";
		
		//组装生产者参数
		Properties prop=new Properties();
		prop.load(ProducerDemo.class.getClassLoader().getResourceAsStream("producer.properties"));

		//创建生产者对象其中的泛型表示为：K，V 表示  此生产者 只能生产key和value类型为此指定的类型的消息
		Producer<String, String> producer=new Producer<String, String>(new ProducerConfig(prop));
		
		//组装message  key：message的key类型  valuemessage 的value类型
		KeyedMessage<String, String> message=new KeyedMessage<String, String>(topic, "msg0");
		
		//key：用于分区。value才是消息的内容
		KeyedMessage<String, String> message1=new KeyedMessage<String, String>(topic,"k1" ,"msg1");
		KeyedMessage<String, String> message2=new KeyedMessage<String, String>(topic,"k2" ,"msg2");
		
		List<KeyedMessage<String, String>> list=new ArrayList<KeyedMessage<String,String>>();
		list.add(message);
		list.add(message1);
		list.add(message2);
		
		//发送
		producer.send(message);
		System.out.println("ok");
		producer.send(list);
		//关闭对象
		producer.close();
		
		
	}
	
	
	
}
