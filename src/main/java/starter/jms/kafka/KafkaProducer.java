package starter.jms.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 多分区
 * <p>
 * $ bin/kafka-topics.sh --create --zookeeper localhost:2181
 * --replication-factor 3 --partitions 2 --topic test
 * </p>
 * <p>
 * test start 3 producers, 2 consumers
 * </p>
 * 
 * @author Yanhong Lee
 * 
 */
public class KafkaProducer extends KafkaThread {

	private kafka.javaapi.producer.Producer<Integer, String> producer;
	private int messageNo = 1;

	public void setup(Properties props) {
		super.setup(props);
		ProducerConfig pcfg = new ProducerConfig(props);
		this.producer = new kafka.javaapi.producer.Producer<Integer, String>(
				pcfg);
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
		while (true) {
			send();
			sleep();
		}
	}

	public static void main(String[] args) {
		KafkaProducer p = new KafkaProducer();
		Properties props = p.createConfig();
		p.setup(props);
		p.start();
	}

}
