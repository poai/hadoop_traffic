package cn.com.zjf.MR_04;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class T {
	public static final TreeMap<Integer, String> tm = new TreeMap<Integer, String>();

	public static void main(String[] args) throws IOException {
		String temp = "CARPLATE=鲁RD369A";
		System.out.println(temp.substring(9));
		tm.put(17774, 1000 + "");
		tm.put(9421, 10002 + "");
		tm.put(17774, 1003 + "");
		tm.put(28600, 1004 + "");
		for (Integer key : tm.keySet()) {
			System.out.println(key + "**" + tm.get(key));

		}
		Configuration conf = new Configuration();
		FileSystem sys = FileSystem.get(conf);
		Path input = new Path("/zjf/data/tvc/*/*");
		FileStatus []stats = sys.globStatus(input);
		System.out.println(Arrays.asList(stats));
	}
	
	@Test
	public void test(){
		System.out.println("2016-12-03 16:10:12 000|1007,".split(",").length);
		String str="2017-01-13 08:59:52 000";
		System.out.println(str.replaceAll("[-\\s:]", "").substring(0,8));
	}

}
