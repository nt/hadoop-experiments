package com.captaindash.jobs;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TwitterClients {

	public static class ClientMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		JSONParser parser = new JSONParser();
		ContainerFactory cf = new ContainerFactory() {
			
			@Override
			public Map createObjectContainer() {
				// TODO Auto-generated method stub
				return new LinkedHashMap();
			}
			
			@Override
			public List creatArrayContainer() {
				// TODO Auto-generated method stub
				return new LinkedList();
			}
		};
		public void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
			Map json;
			try {
				json = (Map)parser.parse(value.toString(), cf);
				Iterator iter = json.entrySet().iterator();
				while(iter.hasNext()){
					Map.Entry entry = (Map.Entry)iter.next();
					//System.out.println(entry.getKey());
					if(entry.getKey().equals("source")) {
						context.write(new Text((String) entry.getValue()), new IntWritable(1));
						break;
					}
				}
			} catch (ParseException e) {
			}

		};
	}
	
	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text text, Iterable<IntWritable> it, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
			int count = 0;
			Iterator<IntWritable> i = it.iterator();
			while(i.hasNext()){
				count += i.next().get();
			}
			System.out.println(text+"\t"+count);
			context.write(text, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job();
		job.setJarByClass(TwitterClients.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ClientMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setCombinerClass(CountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
