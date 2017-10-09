package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		private final static FloatWritable tempo = new FloatWritable();
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				System.out.println("songid");

				// System.out.println(datasplit[43]);
				// System.out.println(datasplit[47]);

				String songID = "";
				float tempoID = 0;

				songID = datasplit[43];
				tempoID = Float.parseFloat(datasplit[47]);

				tempo.set(tempoID);

				context.write(new Text(songID), tempo);
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
		int counter_for_AverageCalc=0;
		float total_Tempo=0;
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			
			for (FloatWritable val : values) {
				sum += val.get();
				counter_for_AverageCalc+=1;
				total_Tempo+=val.get();
			}
			
			float average_tempo=total_Tempo/counter_for_AverageCalc;
			
			result.set(sum);
			
			context.write(new Text("Tatal avergae tempo acorss all songs: "), result);
			
		}
	
		}
		

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(Q2.class);
		job.setMapperClass(Tokeniizermapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
