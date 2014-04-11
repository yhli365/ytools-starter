package starter.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import starter.Constant;

/**
 * @author Yanhong Lee
 * @link http://avro.apache.org/docs/current/gettingstartedjava.html
 */
public class AvroDeSerWithoutCodeGenerationTest {

	private File file = new File(Constant.TEMP_DIR + "users.avro");

	@Test
	public void serialize() throws IOException {
		// First, we use a Parser to read our schema definition and create a
		// Schema object.
		Schema schema = new Parser().parse(new File("src/main/avro/user.avsc"));

		// Serialize user1 and user2 to disk
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);

		List<GenericRecord> users = this.createUsers(schema);
		for (GenericRecord user : users) {
			dataFileWriter.append(user);
		}

		dataFileWriter.close();
	}

	@Test
	public void deserialize() throws IOException {
		deserialize("src/main/avro/user.avsc");
	}

	@Test
	public void deserialize2() throws IOException {
		// 少1个字段
		deserialize("src/main/avro/user2.avsc");
	}

	@Test
	public void deserialize3() throws IOException {
		// 多1个字段，必须有默认值，否则报错
		deserialize("src/main/avro/user3.avsc");
	}

	@Test
	public void deserByteArray() throws IOException {
		Schema schema = new Parser().parse(new File("src/main/avro/user.avsc"));
		// Serializing to a byte array
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);

		List<GenericRecord> users = this.createUsers(schema);
		for (GenericRecord user : users) {
			writer.write(user, encoder);
		}
		encoder.flush();
		out.close();
		byte[] serializedBytes = out.toByteArray();
		System.out.println("toByteArray: " + serializedBytes.length);

		// Deserializing from a byte array
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
				schema);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
				serializedBytes, null);
		GenericRecord user = null;
		while (!decoder.isEnd()) {
			user = reader.read(user, decoder);
			System.out.println(user);
		}
	}

	private List<GenericRecord> createUsers(Schema schema) throws IOException {
		List<GenericRecord> users = new ArrayList<GenericRecord>();

		// Using this schema, let's create some users.
		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);
		// Leave favorite color null
		users.add(user1);

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Ben");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");
		users.add(user2);
		return users;
	}

	private void deserialize(String schemaFile) throws IOException {
		Schema schema = new Parser().parse(new File(schemaFile));
		// Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

}
