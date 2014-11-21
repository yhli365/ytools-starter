package starter.serdes.kryo;

import java.awt.Color;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ColorSerializer extends Serializer<Color> {

	@Override
	public void write(Kryo kryo, Output output, Color object) {
		output.writeInt(object.getRGB());
	}

	@Override
	public Color read(Kryo kryo, Input input, Class<Color> type) {
		return new Color(input.readInt(), true);
	}
}
