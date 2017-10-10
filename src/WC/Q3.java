package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

public class Q3 {

	public static double median(double[] m) {
	    int middle = m.length/2;
	    if (m.length%2 == 1) {
	        return m[middle];
	    } else {
	        return (m[middle-1] + m[middle]) / 2.0;
	    }
	}
	

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		private final static FloatWritable tempo = new FloatWritable();
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				
		
				String songID = "";
				
				float danceability ;

				songID = datasplit[43];
				danceability = Float.parseFloat(datasplit[21]);

				tempo.set(danceability);

				context.write(new Text("song-median"), tempo);
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
		int counter_for_AverageCalc=0;
		float total_Tempo;
		ArrayList<Float> daceabilityList = new ArrayList<Float>();
		
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			
			float sum = 0;
			daceabilityList.clear();
			
			for (FloatWritable val : values) {
				
				daceabilityList.add(val.get());
			
			}
			
			Collections.sort(daceabilityList);
			int size  = daceabilityList.size();

			Float median;
			
			if(size%2 == 0){
				int half = size/2;

				median  = daceabilityList.get(half);
			}else {
				int half = (size + 1)/2;
				median = daceabilityList.get(half -1);
			}
			
			result.set(median);
			
			context.write(new Text("Tatal avergae tempo acorss all songs: " ), result);
			
		}
	
		}
		

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(Q3.class);
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
