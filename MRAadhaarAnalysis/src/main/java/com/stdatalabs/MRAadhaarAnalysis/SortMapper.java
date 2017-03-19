package com.stdatalabs.MRAadhaarAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	Text state = new Text();
	IntWritable count = new IntWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().split("\\|");

		state.set(splits[0].trim());
		count.set(Integer.parseInt(splits[1].trim()));

		context.write(count, state);
	}
}
