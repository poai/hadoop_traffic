package cn.com.zjf.MR_04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarCaptureByonet {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ITGS_LIST");
		job.setJarByClass(cn.com.zjf.MR_04.CarCaptureByonet.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(CarMapper.class);
		job.setReducerClass(CarReduce.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		FileSystem fs = FileSystem.get(conf);
		Path input = new Path(args[0]);
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
					FileInputFormat.addInputPath(job, temp);
				}
			}
		}
		CombineTextInputFormat.setMaxInputSplitSize(job, 67108864);

		Path output = new Path(args[1]);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}
}

class CarMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String temp = value.toString();
		if (temp.length() > 13) {
			temp = temp.substring(12);
			String[] items = temp.split(",");
			if (items.length > 10) {
				// 卡口ID
				context.write(new Text(items[14].substring(6)), value);
			}
		}

	}
}

class CarReduce extends Reducer<Text, Text, NullWritable, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> vlaues, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), key);
	}

}
