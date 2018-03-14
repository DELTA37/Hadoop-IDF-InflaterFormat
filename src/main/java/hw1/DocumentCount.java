package hw1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hw1.InflaterInputFormat;

public class DocumentCount extends Configured implements Tool {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private static final LongWritable one = new LongWritable(1);

		@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> allMatches = new HashSet<>();
      Matcher m = Pattern.compile("\\p{L}+").matcher(value.toString().toLowerCase());
      while (m.find()) {
     		allMatches.add(m.group());
      }
      for (String word : allMatches) {
      	context.write(new Text(word), one);
			}
		}
  }

  public static class DocCountReducer extends Reducer<Text, LongWritable, Text, IntWritable> {
		@Override
    protected void reduce(Text word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
    	int sum = 0;
      for(LongWritable ignored : nums) {
      	sum += 1;
      }
      context.write(word, new IntWritable(sum));
		}
  }

  private Job getJobConf(String inputDir, String outputDir) throws Exception {
		System.out.println(inputDir);
		System.out.println(outputDir);

		getConf().set(TextOutputFormat.SEPERATOR, "\t");
    Job job = Job.getInstance(getConf(), "document count");

    job.setJarByClass(DocumentCount.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(DocCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);		

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    InflaterInputFormat.addInputPath(job, new Path(inputDir));
    TextOutputFormat.setOutputPath(job, new Path(outputDir));

		return job;
  }

	@Override
  public int run(String[] args) throws Exception {
    Job job = getJobConf(args[0], args[1]);
  	return job.waitForCompletion(true) ? 0 : 1;
	}

	static public void main(String[] args) throws Exception {
  	int ret = ToolRunner.run(new DocumentCount(), args);
    System.exit(ret);
	}

}

