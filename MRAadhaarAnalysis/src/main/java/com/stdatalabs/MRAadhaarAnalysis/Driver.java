package com.stdatalabs.MRAadhaarAnalysis;

/*#############################################################################################
# Description: Aadhaar dataset analysis using MapReduce
#
# Input: 
#   1. /user/cloudera/UIDAI-ENR-DETAIL-20170308.csv
#
# To Run this code use the command:    
# yarn jar MRAadhaarAnalysis-0.0.1-SNAPSHOT.jar \
#		   com.stdatalabs.MRAadhaarAnalysis.Driver \
#		   UIDAI-ENR-DETAIL-20170308.csv \
#		   MRStateWiseUIDCount \
#		   MRStateWiseUIDCount_sorted
#############################################################################################*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Usage: [input] [output1] [output2]");
			System.exit(-1);
		}
		Job stateWiseCount = Job.getInstance(getConf());
		stateWiseCount.setJobName("Aadhaar Data Analysis");
		stateWiseCount.setJarByClass(Driver.class);

		/* Field separator for reducer output*/
		stateWiseCount.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		stateWiseCount.setMapperClass(NumUIDMapper.class);
		stateWiseCount.setReducerClass(NumUIDReducer.class);

		stateWiseCount.setInputFormatClass(TextInputFormat.class);
		stateWiseCount.setMapOutputKeyClass(Text.class);
		stateWiseCount.setMapOutputValueClass(IntWritable.class);

		stateWiseCount.setOutputKeyClass(Text.class);
		stateWiseCount.setOutputValueClass(IntWritable.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(stateWiseCount, inputFilePath);
		FileOutputFormat.setOutputPath(stateWiseCount, outputFilePath);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		stateWiseCount.waitForCompletion(true);
		

		Job sort = Job.getInstance(getConf());
		sort.setJobName("Sorting States on Num Aadhaars generated");
		sort.setJarByClass(Driver.class);		
		
		sort.setOutputKeyClass(Text.class);
		sort.setOutputValueClass(IntWritable.class);

		sort.setMapperClass(sortMapper.class);
		sort.setReducerClass(sortReducer.class);
		sort.setSortComparatorClass(sortComparator.class);
		
		sort.setMapOutputKeyClass(IntWritable.class);
		sort.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(sort, new Path(args[1]));
		FileOutputFormat.setOutputPath(sort, new Path(args[2]));

		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}
		
		return sort.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Driver(), args);
	}

}
