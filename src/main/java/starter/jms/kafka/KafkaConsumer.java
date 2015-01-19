package starter.jms.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {

	private ConsumerConnector consumer;
	private String topic;

	private Properties props;
	private SimpleDateFormat df = new SimpleDateFormat(
			"yyyy/MM/dd HH:mm:ss.SSS");

	public void setup(Properties props) {
		ConsumerConfig ccfg = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(ccfg);
		this.topic = props.getProperty("topic", "test");
		this.props = props;
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
		long sleepTime = Long
				.parseLong(props.getProperty("sleep.time", "2000"));
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
			try {
				sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public Properties createConfig() {
		Properties props = new Properties();

		// -----Consumer config
		// 设置zookeeper的链接地址
		props.put("zookeeper.connect", "ys0:2181");
		// 设置group id
		props.put("group.id", "1");
		// kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");

		return props;
	}

	public static void main(String[] args) {
		KafkaConsumer t = new KafkaConsumer();
		try {
			Properties props = t.createConfig();
			t.setup(props);
			t.start();
		} finally {
			t.cleanup();
		}
	}

}
