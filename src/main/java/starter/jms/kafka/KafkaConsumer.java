package starter.jms.kafka;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends KafkaThread {

	private ConsumerConnector consumer;

	public void setup(Properties props) {
		super.setup(props);
		ConsumerConfig ccfg = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(ccfg);
	}

	public void cleanup() {
		if (consumer != null) {
			consumer.shutdown();
		}
		System.out.println("consumer.shutdown********");
	}

	public void receive(int num) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("*********Results********");
		while (it.hasNext()) {
			Date now = new Date(System.currentTimeMillis());
			System.out.println("[" + df.format(now) + "] receive: "
					+ new String(it.next().message()) + " => " + num);
			num--;
			if (num < 1) {
				break;
			}
		}
	}

	@Override
	public void run() {
		// 设置Topic=>Thread Num映射关系, 构建具体的流
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("*********Results********");
		while (it.hasNext()) {
			Date now = new Date(System.currentTimeMillis());
			System.out.println("[" + df.format(now) + "] receive: "
					+ new String(it.next().message()));
			sleep();
		}
	}

	public static void main(String[] args) {
		KafkaConsumer t = new KafkaConsumer();
		try {
			Properties props = t.createConfig();
			t.setup(props);
			t.start();
		} finally {
			// t.cleanup();
		}
	}

}
