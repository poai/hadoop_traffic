package cn.com.zjf.MR_04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
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

/*移动平均计算每天的0-24点卡口流量移动平均值*/
public class CarAvgFlowPerHour {
	public static final String INPUT_DATE_FORMAT = "INPUT_DATE_FORMAT";
	public static final String WINDOW_SIZE = "WINDOW_SIZE";
	public static final String GRANULARITY_SIZE="GRANULARITY_SIZE";
	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		// 2000-01-14 01:08:28
		conf.set(CarAvgFlowPerHour.INPUT_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
		if (args.length >= 3) {
			conf.set(WINDOW_SIZE, args[2]);
		}
		if(args.length>=4){
			conf.set(GRANULARITY_SIZE, args[3]);
		}
		Job job = Job.getInstance(conf, "CarAvgFlowPerHour.java");
		job.setJarByClass(cn.com.zjf.MR_04.CarAvgFlowPerHour.class);

		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(CarAvgFlowMapper.class);
		// 按组合键最小单元数合并
		job.setCombinerClass(CarAvgFlowCombine.class);
		job.setReducerClass(CarAvgFlowReduce.class);

		job.setMapOutputKeyClass(CarAvgOrder.class);
		job.setMapOutputValueClass(IntWritable.class);
		// partitioner
		job.setPartitionerClass(CarAvgPartitioner.class);
		// group no user
		// job.setGroupingComparatorClass(CarAvgComparator.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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

class CarAvgFlowMapper extends Mapper<LongWritable, Text, CarAvgOrder, IntWritable> {
	private SimpleDateFormat sdf;
	// 数据统计粒度 N*分钟
	private Integer granularity = 1;

	@Override
	protected void setup(Mapper<LongWritable, Text, CarAvgOrder, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String format = conf.get(CarAvgFlowPerHour.INPUT_DATE_FORMAT, "yyyy-mm-dd HH:mm:ss");
		granularity=conf.getInt(CarAvgFlowPerHour.GRANULARITY_SIZE, 1);
		sdf = new SimpleDateFormat(format);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CarAvgOrder, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable base = new IntWritable(1);
		String temp = value.toString();
		if (temp.length() > 13) {
			temp = temp.substring(12);
			String[] items = temp.split(",");
			if (items.length > 10) {
				try {
					Date date = sdf.parse(items[0].substring(9));
					CarAvgOrder cao = new CarAvgOrder(new Text(items[14].substring(6)),
							new LongWritable(date.getTime() / 1000 / (60*granularity)));
					context.write(cao, base);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}

class CarAvgFlowCombine extends Reducer<CarAvgOrder, IntWritable, CarAvgOrder, IntWritable> {
	@Override
	protected void reduce(CarAvgOrder cao, Iterable<IntWritable> values,
			Reducer<CarAvgOrder, IntWritable, CarAvgOrder, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable lw : values) {
			sum += lw.get();
		}
		context.write(cao, new IntWritable(sum));
	}
}

class CarAvgFlowReduce extends Reducer<CarAvgOrder, IntWritable, Text, Text> {
	private MultipleOutputs<Text, Text> mo;
	private Integer windowSize = 3;
	private Integer granularity=1;
	private Queue<Integer> queue = new LinkedBlockingQueue<Integer>(windowSize + 1);

	private SimpleDateFormat sdf;

	@Override
	protected void setup(Reducer<CarAvgOrder, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		mo = new MultipleOutputs<Text, Text>(context);
		Configuration conf = context.getConfiguration();
		String format = conf.get(CarAvgFlowPerHour.INPUT_DATE_FORMAT, "yyyy-mm-dd HH:mm:ss");
		sdf = new SimpleDateFormat(format);
		windowSize = conf.getInt(CarAvgFlowPerHour.WINDOW_SIZE, 3);
		granularity=conf.getInt(CarAvgFlowPerHour.GRANULARITY_SIZE, 1);
	}

	//
	@Override
	protected void reduce(CarAvgOrder cao, Iterable<IntWritable> values,
			Reducer<CarAvgOrder, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable lw : values) {
			sum += lw.get();
		}
		String time = sdf.format(new Date(cao.getPassTime().get() * 1000* (60*granularity)));
		// 计算移动平均
		queue.add(sum);
		if (queue.size() > windowSize) {
			queue.poll();
		}
		sum = 0;
		for (Integer item : queue) {
			sum += item;
		}
		int avg_cont = queue.size() == windowSize ? windowSize : queue.size();
		int movAvg = sum / avg_cont;
		mo.write(new Text(String.valueOf((time))), new Text(String.valueOf(movAvg)), cao.getTgsid().toString());
	}

	@Override
	protected void cleanup(Reducer<CarAvgOrder, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		mo.close();
	}
}

class CarAvgOrder implements Writable, WritableComparable<CarAvgOrder> {
	private Text tgsid;
	private LongWritable passTime;

	// MUST
	public CarAvgOrder() {
		tgsid = new Text();
		passTime = new LongWritable();
	}

	public int compareTo(CarAvgOrder order) {
		int result = this.tgsid.compareTo(order.getTgsid());
		if (result == 0) {
			result = passTime.compareTo(order.getPassTime());
		}
		return result;
	}

	public void write(DataOutput out) throws IOException {
		tgsid.write(out);
		passTime.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		tgsid.readFields(in);
		passTime.readFields(in);
	}

	public Text getTgsid() {
		return tgsid;
	}

	public void setTgsid(Text tgsid) {
		this.tgsid = tgsid;
	}

	public LongWritable getPassTime() {
		return passTime;
	}

	public void setPassTime(LongWritable passTime) {
		this.passTime = passTime;
	}

	public CarAvgOrder(Text tgsid, LongWritable passTime) {
		super();
		this.tgsid = tgsid;
		this.passTime = passTime;
	}
}

class CarAvgPartitioner extends Partitioner<CarAvgOrder, LongWritable> {
	@Override
	public int getPartition(CarAvgOrder key, LongWritable value, int numPartitions) {
		return key.getTgsid().hashCode() % numPartitions;
	}
}

class CarAvgComparator extends WritableComparator {

	public CarAvgComparator() {
		// 指定Key值
		super(CarAvgOrder.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CarAvgOrder left = (CarAvgOrder) a;
		CarAvgOrder right = (CarAvgOrder) b;
		return left.getTgsid().compareTo(right.getTgsid());
	}
}
