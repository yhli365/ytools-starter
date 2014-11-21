package starter.serdes.thrift;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import starter.Constant;
import example.thrift.Address;
import example.thrift.Person;

public class ThriftSerDesTest {

	private File file = new File(Constant.TEMP_DIR + "person.tft");

	@Test
	public void serialize() throws TException, IOException {
		TTransport transport = new TIOStreamTransport(null,
				new FileOutputStream(file));
		TProtocol protocol = new TBinaryProtocol(transport);
		for (int i = 0; i < 1000; i++) {
			List<Address> alist = new ArrayList<Address>();
			alist.add(new Address("Line_a1_" + i, "Line_a2_" + i));
			alist.add(new Address("Line_b1_" + i, "Line_b2_" + i));
			Person item = new Person(i, "Name" + i, alist);
			item.write(protocol);
		}
		transport.close();
	}

	@Test
	public void deserialize() throws TException, IOException {
		TTransport transport = new TIOStreamTransport(
				new FileInputStream(file), null);
		TProtocol protocol = new TBinaryProtocol(transport);
		Person item = new Person();
		for (int i = 0; i < 1000; i++) {
			item.read(protocol);
			System.out.println("[" + i + "] = " + item);
		}
		transport.close();
	}
}
