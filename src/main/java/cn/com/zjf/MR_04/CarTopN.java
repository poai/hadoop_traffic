package cn.com.zjf.MR_04;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarTopN {
	public static final String TOPN = "TOPN";
	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Integer N = Integer.parseInt(args[2]);
		Configuration conf = new Configuration();
		// define the N
		conf.setInt(CarTopN.TOPN, N);
		Job job = Job.getInstance(conf, "CAR_Top10_BY_TGSID");
		job.setJarByClass(cn.com.zjf.MR_04.CarTopN.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(CarTopNMapper.class);
		// not use
		// job.setCombinerClass(Top10Combine.class);
		job.setReducerClass(CarTopNReduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(ITGSParition.class);
		FileSystem fs = FileSystem.get(conf);
		// 预处理文件 .只读取写完毕的文件 .writed结尾 .只读取文件大小大于0的文件
		{
			FileStatus childs[] = fs.globStatus(input, new PathFilter() {
				public boolean accept(Path path) {
					if (path.toString().endsWith(".writed")) {
						return true;
					}
					return false;
				}
			});
			Path temp = null;
			for (FileStatus file : childs) {
				temp = new Path(file.getPath().toString().replaceAll(".writed", ""));
				if (fs.listStatus(temp)[0].getLen() > 0) {
					FileInputFormat.addInputPath(job, temp);
				}
			}
		}
		CombineTextInputFormat.setMaxInputSplitSize(job, 67108864);

		// 强制清理输出目录
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}
}

class ITGSParition extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return (Math.abs(key.hashCode())) % numPartitions;
	}
}

class CarTopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String temp = value.toString();
		if (temp.length() > 13) {
			temp = temp.substring(12);
			String[] items = temp.split(",");
			if (items.length > 10) {
				// CarPlate As Key
				try {
					String tgsid = items[14].substring(6);
					Integer.parseInt(tgsid);
					context.write(new Text(tgsid), new IntWritable(1));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

class CarTopNCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
	//有序Map 始终存储TOPN,不存储多余的数据
	private final TreeMap<Integer, String> tm = new TreeMap<Integer, String>();
	private int N;

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		N = conf.getInt(CarTopN.TOPN, 10);
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		Integer weight = 0;
		for (IntWritable iw : values) {
			weight += iw.get();
		}
		tm.put(weight, key.toString());
		//保证只有TOPN
		if (tm.size() > N) {
			tm.remove(tm.firstKey());
		}
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//将最终的数据进行发射输出给下一阶段
		for (Integer key : tm.keySet()) {
			context.write(new Text("byonet:" + tm.get(key)), new IntWritable(key));
		}
	}
}

// Top10核心计算方法
// 尽量避免在Java集合中存储Hadoop数据类型,可能会出现奇怪的问题
class CarTopNReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final TreeMap<Integer, String> tm = new TreeMap<Integer, String>();
	private int N;

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		N = conf.getInt(CarTopN.TOPN, 10);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		Integer weight = 0;
		for (IntWritable iw : values) {
			weight += iw.get();
		}
		tm.put(weight, key.toString());
		if (tm.size() > N) {
			tm.remove(tm.firstKey());
		}
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		for (Integer key : tm.keySet()) {
			context.write(new Text("byonet:" + tm.get(key)), new IntWritable(key));
		}
	}
}
