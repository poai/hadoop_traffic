package cn.com.zjf.MR_04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.zjf.MR_04.count.MR;

public class Car2 extends Configuration implements Tool {
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new Car2(), args);
		System.exit(res);
	}

	public void setConf(Configuration conf) {
		conf.set("libjars", "/lib/ojdbc14.jar");
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "oracle.jdbc.driver.OracleDriver");
	}

	public Configuration getConf() {
		return this;
	}

	public int run(String[] args) throws Exception {

		System.out.println(Arrays.asList(args));
		DBConfiguration.configureDB(this, "oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@10.150.27.233:1521:PROD", "tvc_itgs_test", "ehl1234");

		Job job = Job.getInstance(this, "CAR_FROM_ORACLE");
		DBInputFormat.setInput(job, OracleInput.class, "select hphm,gcsj,kkbh from T_ITGS_PASSCAR_SHARE",
				"select count(1) from T_ITGS_PASSCAR_SHARE");

		job.setInputFormatClass(DBInputFormat.class);
		job.setJarByClass(cn.com.zjf.MR_04.Car2.class);
		job.setMapperClass(DBMapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path  oracle=new Path("/lib/ojdbc14.jar");	
		job.addFileToClassPath(oracle);
		// InputSampler.Sampler<IntWritable, Text> sampler=new
		// InputSampler.RandomSampler<IntWritable, Text>(0.1,10000,10);
		// InputSampler.writePartitionFile(job, sampler);
		//
		FileSystem system = FileSystem.get(this);
		Path output = new Path("/zjf/output6");
		system.delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		job.submit();
		return 0;
	}
}

class DBMapper extends Mapper<LongWritable, OracleInput, Text, Text> {
	public static Log log = LogFactory.getLog(Car2.class);
	@Override
	protected void map(LongWritable key, OracleInput value,
			Mapper<LongWritable, OracleInput, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println(value);
		log.debug(value.toString());
		context.write(new Text(value.getHphm()), new Text(value.getTgsj()));
		if (value.getHphm().equals("京A00002")) {
			context.getCounter(MR.MAPPER_COUNT).increment(1);
			context.getCounter("tvc", "TEST_COUNT").increment(1);
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, OracleInput, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("-----------------:" + context.getCounter(MR.MAPPER_COUNT).getValue());
	}
}

class OracleInput implements DBWritable, Writable {

	private String hphm;
	private String tgsj;
	private String kkbh;

	public String getHphm() {
		return hphm;
	}

	public void setHphm(String hphm) {
		this.hphm = hphm;
	}

	public String getTgsj() {
		return tgsj;
	}

	public void setTgsj(String tgsj) {
		this.tgsj = tgsj;
	}

	public String getKkbh() {
		return kkbh;
	}

	public void setKkbh(String kkbh) {
		this.kkbh = kkbh;
	}

	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, hphm);
		statement.setString(2, tgsj);
		statement.setString(3, kkbh);

	}

	public void readFields(ResultSet resultSet) throws SQLException {
		this.hphm = resultSet.getString(1);
		this.tgsj = resultSet.getString(2);
		this.kkbh = resultSet.getString(3);
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, hphm);
		Text.writeString(out, tgsj);
		Text.writeString(out, kkbh);

	}

	public void readFields(DataInput in) throws IOException {
		this.hphm = Text.readString(in);
		this.tgsj = Text.readString(in);
		this.kkbh = Text.readString(in);
	}

	@Override
	public String toString() {
		return "OracleInput [hphm=" + hphm + ", tgsj=" + tgsj + ", kkbh=" + kkbh + "]";
	}

}
