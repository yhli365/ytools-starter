package starter;

import java.lang.reflect.Method;

public class TestUtil {

	public static void execMain(Class<?> mainClass, String... commandLine)
			throws Exception {
		String[] args;
		if (commandLine == null) {
			args = new String[0];
		} else if (commandLine.length == 1) {
			args = commandLine[0].split("\\s+");
		} else {
			args = commandLine;
		}

		Method main = null;
		try {
			Class<?>[] paramTypes = new Class<?>[] { String[].class };
			main = mainClass.getMethod("main", paramTypes);
			main.invoke(null, new Object[] { args });
		} catch (NoSuchMethodException e) {
			e.printStackTrace(System.out);
		}
	}

}
