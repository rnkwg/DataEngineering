import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class UBERStudent20180941 {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word1 = new Text();
		private Text word2 = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(",");
			
			String number = values[0];
			String date = values[1];
			int vehicle = Integer.parseInt(values[2]);
			int trip = Integer.parseInt(values[3]);
			
			String dayOfWeek = "";
			try {	
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
				Date nDate = dateFormat.parse(date);
			
				Calendar cal = Calendar.getInstance();
				cal.setTime(nDate);
			
				int dayNum = cal.get(Calendar.DAY_OF_WEEK);
			
				switch(dayNum) { 
					case 1: 
						dayOfWeek = "SUN"; 
						break; 
					case 2: 
						dayOfWeek = "MON"; 
						break; 
					case 3: 
						dayOfWeek = "TUE"; 
						break; 
					case 4: 
						dayOfWeek = "WED";
						break; 
					case 5: 
						dayOfWeek = "THR"; 
						break; 
					case 6: 
						dayOfWeek = "FRI"; 
						break; 
					case 7: 
						dayOfWeek = "SAT"; 
						break; 
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			
		    	word1.set(number + "," + dayOfWeek);
		    	word2.set(trip + "," + vehicle); 
		    	context.write(word1, word2);
		}
	}

	public static class WordCountReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int totalTrips = 0;
			int totalVehicles = 0;
			
            			for (Text val : values) {
            				String[] itr = val.toString().split(",");
            				int trip = Integer.parseInt(itr[0]);
            				int vehicle = Integer.parseInt(itr[1]);

            				totalTrips += trip;
            				totalVehicles += vehicle;
            			}

            			result.set(totalTrips + "," + totalVehicles);
            			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UBERStudent20180941 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20180941");
		job.setJarByClass(UBERStudent20180941.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
