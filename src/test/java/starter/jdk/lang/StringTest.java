package starter.jdk.lang;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class StringTest {

	@Test
	public void testReplace() {
		String[] tests = new String[] { "${srcname}.seq",//
				"bcp_${timestamp}_${srcname}.lzo_deflate",//
				"bcp_${datetime}_${srcname}.lzo_deflate" };
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		for (String t : tests) {
			System.out.println("\n------------");
			System.out.println("test: " + t);
			String dst = t.replaceAll("\\$\\{srcname\\}", "filename");
			dst = dst.replaceAll("\\$\\{timestamp\\}",
					String.valueOf(System.currentTimeMillis()));
			dst = dst.replaceAll("\\$\\{datetime\\}", df.format(new Date()));
			System.out.println("dst: " + dst);
		}
	}

	@Test
	public void testReplace2() {
		String[] tests = new String[] { "#(srcname).seq",//
				"bcp_#(timestamp)_#(srcname).lzo_deflate",//
				"bcp_#(datetime)_#(srcname).lzo_deflate" };
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		for (String t : tests) {
			System.out.println("\n------------");
			System.out.println("test: " + t);
			String dst = t.replaceAll("\\#\\(srcname\\)", "filename");
			dst = dst.replaceAll("\\#\\(timestamp\\)",
					String.valueOf(System.currentTimeMillis()));
			dst = dst.replaceAll("\\#\\(datetime\\)", df.format(new Date()));
			System.out.println("dst: " + dst);
		}
	}

}
