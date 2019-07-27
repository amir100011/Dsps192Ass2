package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class FirstStepValue implements WritableComparable<FirstStepValue> { 
	private LongWritable cW1;
	private LongWritable cW1W2;


	public FirstStepValue() {
		this.cW1 = new LongWritable(0);
		this.cW1W2 = new LongWritable(0);
	}

	public FirstStepValue(LongWritable cw1, LongWritable cw1w2) {
		this.cW1 = new LongWritable(cw1.get());
		this.cW1W2 = new LongWritable(cw1w2.get());
	}

	public void setValues(long cw1, long cw1w2) {
		this.cW1 = new LongWritable(cw1);
		this.cW1W2 = new LongWritable(cw1w2);
	}	

	public LongWritable getCW1() {
		return this.cW1;
	}

	public LongWritable getCW1W2() {
		return this.cW1W2;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		cW1W2.write(out);
		cW1.write(out);

	}
	@Override
	public void readFields(DataInput in) throws IOException {
		cW1W2.readFields(in);
		cW1.readFields(in);

	}
	@Override
	public int compareTo(FirstStepValue o) {
		int wordCompare = this.cW1W2.compareTo(o.getCW1W2());
		if( wordCompare == 0) {
			return this.cW1.compareTo(o.getCW1());
		}
		return wordCompare;
	}

	public String toString() {
		return this.cW1.toString() + " " + this.cW1W2.toString();
	}

}