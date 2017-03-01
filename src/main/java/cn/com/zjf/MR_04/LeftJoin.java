package cn.com.zjf.MR_04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LeftJoin extends Configuration implements Tool {

	public static void main(String[] args) throws Exception {
		LeftJoin leftJoin=new LeftJoin();
		ToolRunner.run(leftJoin, leftJoin, args);
	}

	public void setConf(Configuration conf) {
	}

	public Configuration getConf() {
		return this;
	}

	public int run(String[] args) throws Exception {

		System.out.println(Arrays.asList(args));

		Job job = Job.getInstance(this, "CAR_FROM_ORACLE");
		job.setJarByClass(cn.com.zjf.MR_04.Car2.class);
		DBConfiguration.configureDB(this, "oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@10.150.27.233:1521:PROD", "tvc_itgs_test", "ehl1234");
		DBInputFormat.setInput(job, OracleInput.class, "select KKID,KKMC from T_ITGS_TGSINFO ",
				"select count(1) from T_ITGS_TGSINFO ");
		//MultipleInputs.addInputPath(job, new Path(args[0]), CombineTextInputFormat.class, PassCarMapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[0]), DBInputFormat.class, PassCarItgs.class);
		job.setMapperClass(PassCarItgs.class);
		job.setInputFormatClass(DBInputFormat.class);
		job.setReducerClass(LeftJoinReduce.class);
		job.setMapOutputKeyClass(PassCarMode.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// InputSampler.Sampler<IntWritable, Text> sampler=new
		// InputSampler.RandomSampler<IntWritable, Text>(0.1,10000,10);
		// InputSampler.writePartitionFile(job, sampler);
		//
		FileSystem system = FileSystem.get(this);
		Path output = new Path("/zjf/output6");
		system.delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		return 0;
	}
}

class PassCarMapper extends Mapper<LongWritable, Text, PassCarMode, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PassCarMode, Text>.Context context)
			throws IOException, InterruptedException {
		/* 原始文件信息 */
		String temp = value.toString();
		if (temp.length() > 13) {
			temp = temp.substring(12);
			String[] items = temp.split(",");
			if (items.length > 10) {
				// CarPlate As Key
				if (!items[2].endsWith("无牌")) {
					try {
						String kkid = items[14].substring(6);
						// time + tgsid
						context.write(new PassCarMode(new Text(kkid), new Text("1")),
								new Text(items[2].substring(9) + "|" + new Text(items[0].substring(9))));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

	}
}

class PassCarItgs extends Mapper<LongWritable, ItgsInput, PassCarMode, Text> {
	@Override
	protected void map(LongWritable key, ItgsInput value,
			Mapper<LongWritable, ItgsInput, PassCarMode, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(new PassCarMode(new Text(value.getKkid()), new Text("2")), new Text(value.getKkmc()));
	}

}

class LeftJoinReduce extends Reducer<PassCarMode, Text, Text, Text> {
	@Override
	protected void reduce(PassCarMode key, Iterable<Text> value, Reducer<PassCarMode, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		System.out.println(key);
	}
}

class ItgsInput implements DBWritable, Writable {
	private String kkid;
	private String kkmc;

	public String getKkid() {
		return kkid;
	}

	public void setKkid(String kkid) {
		this.kkid = kkid;
	}

	public String getKkmc() {
		return kkmc;
	}

	public void setKkmc(String kkmc) {
		this.kkmc = kkmc;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(getKkid());
		out.writeUTF(getKkmc());
	}

	public void readFields(DataInput in) throws IOException {
		kkid = in.readUTF();
		kkmc = in.readUTF();
	}

	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, kkid);
		statement.setString(2, kkmc);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		kkid = resultSet.getString(1);
		kkmc = resultSet.getString(2);
	}
}

class PassCarMode implements Writable, WritableComparable<PassCarMode> {

	private Text key;
	private Text flag;

	public PassCarMode(Text key, Text flag) {
		super();
		this.key = key;
		this.flag = flag;
	}

	public int compareTo(PassCarMode pcm) {
		int compareValue = this.key.compareTo(pcm.getKey());
		if (compareValue == 0) {
			compareValue = this.flag.compareTo(pcm.getFlag());
		}
		return compareValue;
	}

	public void write(DataOutput out) throws IOException {
		key.write(out);
		flag.write(out);
	}

	public void readFields(DataInput in) throws IOException {

	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getFlag() {
		return flag;
	}

	public void setFlag(Text flag) {
		this.flag = flag;
	}

}
