package starter.serdes.thrift;

import org.junit.Test;

import starter.TestUtil;

public class JavaClientTest {

	@Test
	public void simpleClient() throws Exception {
		TestUtil.execMain(JavaClient.class);
	}

	@Test
	public void secureClient() throws Exception {
		TestUtil.execMain(JavaClient.class, "secure");
	}

}
