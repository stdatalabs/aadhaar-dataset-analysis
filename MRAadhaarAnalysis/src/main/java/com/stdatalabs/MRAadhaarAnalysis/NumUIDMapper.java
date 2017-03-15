package com.stdatalabs.MRAadhaarAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class NumUIDMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text state = new Text();
	IntWritable count = new IntWritable();
	private static final Logger LOG = Logger.getLogger(NumUIDMapper.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] list = value.toString().split(",");

		if (key.get() != 0) {
			state.set(list[2]);
			count.set(Integer.parseInt(list[8]));
			LOG.info("Reading line: " + state + "Aadhaars generated: "
					+ list[8]);
			context.write(state, count);
		}
		LOG.info("Skipped line with key:" + key.get());
	}

}
