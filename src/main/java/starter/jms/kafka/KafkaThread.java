package starter.jms.kafka;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;

public class KafkaThread extends Thread {

	protected String topic;

	protected Properties props;
	protected SimpleDateFormat df = new SimpleDateFormat(
			"yyyy/MM/dd HH:mm:ss.SSS");
	protected Random rand = new Random();

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

		// -----Consumer config
		// 设置zookeeper的链接地址
		props.put("zookeeper.connect", "ys0:2181");
		// 设置group id
		props.put("group.id", "test-g1");
		// kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");

		return props;
	}

	public void setup(Properties props) {
		this.topic = props.getProperty("topic", "test");
		this.props = props;
	}

	public void sleep() {
		int sleepStart = Integer
				.parseInt(props.getProperty("sleep.start", "1"));
		int sleepLen = Integer.parseInt(props.getProperty("sleep.len", "5"));
		try {
			int interval = rand.nextInt(sleepLen);
			sleep((sleepStart + interval) * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
