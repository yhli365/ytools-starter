package starter.serdes.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoCopyable;

public class SomeClass implements KryoCopyable<SomeClass> {
	// ...
	private String name;
	private int age;

	public SomeClass(String name, int age) {
		this.name = name;
		this.age = age;
	}

	private SomeClass() {
	}

	@Override
	public SomeClass copy(Kryo kryo) {
		// Create new instance and copy values from this instance.
		SomeClass b = new SomeClass();
		b.name = name;
		b.age = age;
		return b;
	}

	public String toString() {
		return super.toString() + "[name=" + name + ", age=" + age + "]";
	}

}
