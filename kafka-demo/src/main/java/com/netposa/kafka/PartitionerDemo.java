package com.netposa.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class PartitionerDemo implements Partitioner{

	private VerifiableProperties prop;
	
	@Override
	public int partition(Object key, int numPartitions) {
		
		//System.out.println(prop.getString("producer.type"));
		
		// key 指的是 消息的key  numPartitions指的是此topic中的partition数目 
		//返回值就是分区的索引 0 1 2 3 等
		String key_str=(String) key;
		//System.out.println(key_str);
		if(key_str.contains("1")){
			return 1;
		}
		if(key_str.contains("2")){
			return 2;
		}
		return 0;
		
	}

	public PartitionerDemo(VerifiableProperties prop) {
		this.prop = prop;
	}

}
