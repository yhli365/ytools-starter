package starter.serdes.avro;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import example.avro.proto.Mail;
import example.avro.proto.Message;

/**
 * @author Yanhong Lee
 * @link https://github.com/phunt/avro-rpc-quickstart
 * 
 */
public class AvroRPCTest {

	private String host = "192.168.56.101";
	private int port = 9090;//65111;

	public static class MailImpl implements Mail {
		// in this simple example just return details of the message
		public Utf8 send(Message message) {
			System.out.println("Sending message");
			Utf8 msg = new Utf8("Sending message to "
					+ message.getTo().toString() + " from "
					+ message.getFrom().toString() + " with body "
					+ message.getBody().toString());
			System.out.println("Msg: " + msg);
			return msg;
		}
	}

	@Test
	public void startServer() throws InterruptedException {
		System.out.println("Starting server");
		Server server = new NettyServer(new SpecificResponder(Mail.class,
				new MailImpl()), new InetSocketAddress(port));
		// the server implements the Mail protocol (MailImpl)
		System.out.println("Server started");
		// usually this would be another app, but for simplicity
		Thread.sleep(60000L);
		// cleanup
		server.close();
		System.out.println("Stop server");
	}

	@Test
	public void startClient() throws IOException {
		System.out.println("Starting client");
		NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(host,
				port));
		// client code - attach to the server and send a message
		Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, client);
		System.out.println("Client built, got proxy");

		// fill in the Message record and send it
		Message message = new Message();
		message.setTo(new Utf8("to@qq.com"));
		message.setFrom(new Utf8("from@qq.com"));
		message.setBody(new Utf8("body-" + new Date()));
		System.out.println("Calling proxy.send with message:  "
				+ message.toString());
		System.out.println("Result: " + proxy.send(message));

		// cleanup
		client.close();
		System.out.println("Stop client");
	}

}
