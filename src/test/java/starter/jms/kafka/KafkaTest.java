package starter.jms.kafka;

import java.util.Properties;

import org.junit.Test;

public class KafkaTest {
	protected int num = 10;

	@Test
	public void runProducer() throws InterruptedException {
		KafkaProducer p = new KafkaProducer();
		Properties props = p.createConfig();
		p.setup(props);
		for (int i = 0; i < num; i++) {
			p.send();
		}
		p.cleanup();
	}

	@Test
	public void runConsumer() throws InterruptedException {
		KafkaConsumer p = new KafkaConsumer();
		Properties props = p.createConfig();
		p.setup(props);
		p.receive(num * 2);
		p.cleanup();
	}

}
