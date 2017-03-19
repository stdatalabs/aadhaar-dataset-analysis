package com.stdatalabs.MRAadhaarAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
	
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		IntWritable v1 = (IntWritable) k1;
		IntWritable v2 = (IntWritable) k2;
		
		return v1.get() < v2.get() ? 1 : v1.get() == v2.get() ? 0 : -1; 
		
	}

	protected SortComparator() {
        super(IntWritable.class, true);
    }
}
