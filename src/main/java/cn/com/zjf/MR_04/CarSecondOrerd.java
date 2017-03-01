package cn.com.zjf.MR_04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CarSecondOrerd {
	public static void main(String[] args) throws Exception {

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();
		conf.set("mapreduce.reduce.memory.mb", "4096");
		Job job = Job.getInstance(conf, "TRACK_BY_TIME_TGSID");
		// 小文件合并
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setJarByClass(cn.com.zjf.MR_04.CarSecondOrerd.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(CarOrder.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(CarOrderMap.class);
		job.setReducerClass(CarOrderReduce.class);
		job.setPartitionerClass(CarOrderPartitioner.class);
		job.setGroupingComparatorClass(CarOrderComparator.class);
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
					CombineTextInputFormat.addInputPath(job, temp);
				}
			}
		}
		CombineTextInputFormat.setMaxInputSplitSize(job, 67108864);

		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		if (!job.waitForCompletion(true))
			return;
	}
}

class CarOrderMap extends Mapper<LongWritable, Text, CarOrder, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp = value.toString();
		if (temp.length() > 13) {
			temp = temp.substring(12);
			String[] items = temp.split(",");
			if (items.length > 10) {
				// CarPlate As Key
				if (!items[2].endsWith("无牌")) {
					try {
						CarOrder co = new CarOrder(new Text(items[2].substring(9)), new Text(items[0].substring(9)));
						// time + tgsid
						context.write(co, new Text(items[0].substring(9) + "-" + items[14].substring(6)));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
// Combine

class CarOrderCombine extends Reducer<CarOrder, Text, CarOrder, Text> {
	@Override
	protected void reduce(CarOrder co, Iterable<Text> values, Reducer<CarOrder, Text, CarOrder, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuffer buf = new StringBuffer();
		String before = null;
		String current = null;
		for (Text text : values) {
			current = text.toString();
			if (current.equals(before)) {
				continue;
			}
			buf.append(current);
			buf.append(',');
			before = current;
		}
		if (buf.length() == 0) {
			return;
		}
		context.write(co, new Text(buf.toString()));
	}
}

// Reduce
class CarOrderReduce extends Reducer<CarOrder, Text, Text, Text> {

	MultipleOutputs<Text, Text> mo;

	@Override
	protected void setup(Reducer<CarOrder, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		mo = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(CarOrder key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer buf = new StringBuffer();
		for (Text text : values) {
			buf.append(text.toString());
			buf.append(',');
		}
		String value = buf.toString();
		String[] flows = value.split(",");
		if (flows.length >= 3) {
			String prefix = key.getDay().toString().replaceAll("[-\\s:]", "");
			mo.write(key.getCarPlate(), new Text(value.substring(0, value.length() - 1)), prefix.substring(0, 8));
		}
	}

	@Override
	protected void cleanup(Reducer<CarOrder, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		mo.close();
	}
}

// 组合键定义
class CarOrder implements Writable, WritableComparable<CarOrder> {
	// 号牌
	private Text carPlate;
	// 过车时间
	private Text day;

	public Text getDay() {
		return day;
	}

	public void setDay(Text day) {
		this.day = day;
	}

	public CarOrder() {
		carPlate = new Text();
		day = new Text();
	}

	public CarOrder(Text carPlate, Text day) {
		super();
		this.carPlate = carPlate;
		this.day = day;
	}

	public int compareTo(CarOrder co) {
		int compareValue = this.carPlate.compareTo(co.carPlate);
		// 相等
		if (compareValue == 0) {
			compareValue = this.day.compareTo(co.day);
		}
		return compareValue;
	}

	public void write(DataOutput out) throws IOException {
		this.carPlate.write(out);
		this.day.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.carPlate.readFields(in);
		this.day.readFields(in);
	}

	public Text getCarPlate() {
		return carPlate;
	}

	public void setCarPlate(Text carPlate) {
		this.carPlate = carPlate;
	}

	@Override
	public String toString() {
		return "CarOrder [carPlate=" + carPlate + ", day=" + day + "]";
	}

}
// 定义分区函数

class CarOrderPartitioner extends Partitioner<CarOrder, Text> {
	@Override
	public int getPartition(CarOrder key, Text value, int numPartitions) {
		return Math.abs(key.getCarPlate().hashCode()) % numPartitions;
	}
}

// Comparator
class CarOrderComparator extends WritableComparator {
	public CarOrderComparator() {
		// 指定Key值
		super(CarOrder.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CarOrder car1 = (CarOrder) a;
		CarOrder car2 = (CarOrder) b;
		return car1.getCarPlate().compareTo(car2.getCarPlate());
	}
}
