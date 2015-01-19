package starter.jms.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer extends Thread {

	private kafka.javaapi.producer.Producer<Integer, String> producer;
	private String topic;

	private Properties props;
	private SimpleDateFormat df = new SimpleDateFormat(
			"yyyy/MM/dd HH:mm:ss.SSS");
	private int messageNo = 1;

	public void setup(Properties props) {
		ProducerConfig pcfg = new ProducerConfig(props);
		this.producer = new kafka.javaapi.producer.Producer<Integer, String>(
				pcfg);
		this.topic = props.getProperty("topic", "test");
		this.props = props;
	}

	public void send() {
		Date now = new Date(System.currentTimeMillis());
		String messageStr = "Message_" + messageNo + "_@" + df.format(now);
		System.out.println("[" + df.format(now) + "] Send:" + messageStr);
		producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
		messageNo++;
	}

	public void cleanup() {
		if (producer != null) {
			producer.close();
		}
		System.out.println("producer.close********");
	}

	@Override
	public void run() {
		long sleepTime = Long
				.parseLong(props.getProperty("sleep.time", "2000"));
		while (true) {
			send();
			try {
				sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public Properties createConfig() {
		Properties props = new Properties();
		// -----Producer config
		// 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
		props.put("metadata.broker.list", "ys0:9092");
		// props.put("metadata.broker.list", "ys0:9092,ys0:9093,s1:9094");
		// 配置消息value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置消息key的序列化类
		// props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// 设置Partition类, 对队列进行合理的划分
		// props.put("partitioner.class", "idoall.testkafka.Partitionertest");
		// props.put("num.partitions", "3");
		// ACK机制, 消息发送需要kafka服务端确认
		// request.required.acks
		// 0, which means that the producer never waits for an acknowledgement
		// from the broker (the same behavior as 0.7). This option provides the
		// lowest latency but the weakest durability guarantees (some data will
		// be lost when a server fails).
		// 1, which means that the producer gets an acknowledgement after the
		// leader replica has received the data. This option provides better
		// durability as the client waits until the server acknowledges the
		// request as successful (only messages that were written to the
		// now-dead leader but not yet replicated will be lost).
		// -1, which means that the producer gets an acknowledgement after all
		// in-sync replicas have received the data. This option provides the
		// best durability, we guarantee that no messages will be lost as long
		// as at least one in sync replica remains.

		props.put("request.required.acks", "-1");

		return props;
	}

	public static void main(String[] args) {
		KafkaProducer p = new KafkaProducer();
		Properties props = p.createConfig();
		p.setup(props);
		p.start();
	}

}
