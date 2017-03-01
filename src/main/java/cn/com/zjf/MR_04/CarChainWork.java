package cn.com.zjf.MR_04;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * [MAP+ / REDUCE MAP*]  ==>流水线式作业
 */
public class CarChainWork {
	public static void main(String[] args) throws Exception {
		// in&out
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		// Config
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TEST_CHAIN");
		job.setJarByClass(cn.com.zjf.MR_04.CarChainWork.class);
		ChainMapper.addMapper(job, TMapper1.class, LongWritable.class, Text.class, Text.class, LongWritable.class,
				conf);
		ChainMapper.addMapper(job, TMapper2.class, Text.class, LongWritable.class, Text.class, LongWritable.class,
				conf);
		job.setCombinerClass(TCombine.class);
		ChainReducer.setReducer(job, TReduce.class, Text.class, LongWritable.class, Text.class, LongWritable.class,
				conf);
		FileSystem fs = FileSystem.get(conf);
		/**
		 * 预处理文件 .只读取写完毕的文件 .writed结尾 .只读取文件大小大于0的文件
		 */
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
					CombineTextInputFormat.addInputPath(job, temp);
				}
			}
		}
		CombineTextInputFormat.setMaxInputSplitSize(job, 67108864);
		// distory the output  directory
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);
		if (!job.waitForCompletion(true))
			return;
	}
}

class TMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		LongWritable count = new LongWritable(1);
		String[] strs = value.toString().split(",");
		System.out.println(1);
		for (String str : strs) {
			context.write(new Text(str), count);
		}
	}
}

class TMapper2 extends Mapper<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void map(Text value, LongWritable key, Mapper<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println(2);
		context.write(value, key);
	}
}

class TCombine extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable item : values) {
			sum += item.get();
		}
		context.write(key, new LongWritable(sum));
	}

}

class TReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

	LRUMap map = null;

	@Override
	protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		map = new LRUMap(10);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable item : values) {
			sum += item.get();
		}
		map.put(key, sum);
	}

	@Override
	protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		@SuppressWarnings("unchecked")
		Iterator<Object> it = map.keySet().iterator();
		while (it.hasNext()) {
			Object key = it.next();
			Object value = map.get(key);
			context.write(new Text(key.toString()), new LongWritable(Long.parseLong(value.toString())));
		}

	}
}
