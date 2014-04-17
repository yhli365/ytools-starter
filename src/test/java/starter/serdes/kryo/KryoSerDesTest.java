package starter.serdes.kryo;

import java.awt.Color;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import starter.Constant;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;

/**
 * @author Yanhong Lee
 * @link https://github.com/EsotericSoftware/kryo#quickstart
 * 
 */
public class KryoSerDesTest {

	private File file = new File(Constant.TEMP_DIR + "simple.kyro");

	public static class Simple implements Serializable {
		private static final long serialVersionUID = 9071179835819737926L;
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String toString() {
			return "Simple[name=" + name + ", age=" + age + "]";
		}

	}

	public Kryo getKryo() {
		Kryo kryo = new Kryo();

		// #Registration 注册序列化类提高效率.
		kryo.setRegistrationRequired(true);
		kryo.register(Simple.class);
		kryo.register(ArrayList.class);

		// #Default serializers
		kryo.register(Color.class, new ColorSerializer());
		// kryo.addDefaultSerializer(Color.class, ColorSerializer.class);

		// #References
		// kryo.setReferences(false);

		// #Object creation
		// kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		// kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
		// Registration registration = kryo.register(SomeClass.class);
		// registration.setObjectInstantiator(...);

		// #Context
		kryo.getContext();
		kryo.getGraphContext();

		// #Compression and encryption
		// DeflateSerializer
		// BlowfishSerializer

		// #Compatibility
		// kryo.setDefaultSerializer(TaggedFieldSerializer.class);
		// kryo.setDefaultSerializer(CompatibleFieldSerializer.class);

		// #Stack size
		// The stack size can be increased using -Xss, but note that this is for
		// all threads. Large stack sizes in a JVM with many threads may use a
		// large amount of memory.

		// #Threading
		// Kryo is not thread safe. Each thread should have its own Kryo, Input,
		// and Output instances.

		// #Logging
		// MinLog logging library: https://github.com/EsotericSoftware/minlog

		// #Projects using Kryo
		// KryoNet (NIO networking) https://github.com/EsotericSoftware/kryonet

		return kryo;
	}

	@Test
	public void serSimple() throws IOException {
		Simple obj = new Simple();
		obj.setAge(10);
		obj.setName("kitty");
		System.out.println("source object: " + obj);

		// 序列化
		Output output = new Output(new FileOutputStream(file));
		Kryo kryo = this.getKryo();
		kryo.writeObject(output, obj);
		output.flush();
		output.close();
		System.out.println("ser size: " + file.length());
	}

	@Test
	public void desSimple() throws IOException {
		// 反序列化
		Input input = new Input(new FileInputStream(file));
		Kryo kryo = this.getKryo();
		Simple obj = kryo.readObject(input, Simple.class);
		input.close();
		System.out.println("dest object: " + obj);
	}

	@Test
	public void serSimpleList() throws IOException {
		List<Simple> objList = new ArrayList<Simple>();
		for (int i = 0; i < 5; i++) {
			Simple obj = new Simple();
			obj.setAge(10 + i);
			obj.setName("kitty" + i);
			objList.add(obj);
		}
		System.out.println("source object: " + objList);

		// 序列化
		Output output = new Output(new FileOutputStream(file));
		Kryo kryo = this.getKryo();
		kryo.writeObject(output, objList);
		output.flush();
		output.close();
		System.out.println("ser size: " + file.length());
	}

	@Test
	public void desSimpleList() throws IOException {
		// 反序列化
		Input input = new Input(new FileInputStream(file));
		Kryo kryo = this.getKryo();
		@SuppressWarnings("unchecked")
		List<Simple> objList = kryo.readObject(input, ArrayList.class);
		input.close();
		System.out.println("dest object: " + objList);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void serdesByteArray() throws IOException {
		List<Simple> objList = new ArrayList<Simple>();
		for (int i = 0; i < 5; i++) {
			Simple obj = new Simple();
			obj.setAge(10 + i);
			obj.setName("kitty" + i);
			objList.add(obj);
		}
		System.out.println("source object: " + objList);

		byte[] objBytes;
		Kryo kryo = this.getKryo();

		// 序列化
		Output output = new Output(1024);
		kryo.writeObject(output, objList);
		output.flush();
		objBytes = output.toBytes();
		System.out.println("ser size: " + objBytes.length);

		// 反序列化
		Input input = new Input(objBytes);
		objList = kryo.readObject(input, ArrayList.class);
		input.close();
		System.out.println("dest object: " + objList);
	}

	@Test
	public void copy() {
		Kryo kryo = this.getKryo();
		// #Copying/cloning
		SomeClass someObject = new SomeClass("test", 100);
		System.out.println("s1=" + someObject);
		SomeClass copy1 = kryo.copy(someObject);
		System.out.println("c1=" + copy1);
		SomeClass copy2 = kryo.copyShallow(someObject);
		System.out.println("c2=" + copy2);
	}

	@Test
	public void chunked() throws IOException {
		// #Chunked encoding

		OutputStream outputStream = new FileOutputStream("file.bin");
		OutputChunked output = new OutputChunked(outputStream, 1024);
		// Write data to output...
		output.endChunks();
		// Write more data to output...
		output.endChunks();
		// Write even more data to output...
		output.close();

		InputStream inputStream = new FileInputStream("file.bin");
		InputChunked input = new InputChunked(inputStream, 1024);
		// Read data from first set of chunks...
		input.nextChunks();
		// Read data from second set of chunks...
		input.nextChunks();
		// Read data from third set of chunks...
		input.close();

	}
}
