package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class FirstJobValue implements WritableComparable<FirstJobValue>{

	private LongWritable pairCount;
	private LongWritable wordOccurrences;
	
	public FirstJobValue() {
		pairCount = new LongWritable();
		wordOccurrences = new LongWritable();
	}
	
	public FirstJobValue(LongWritable pairCount,LongWritable WordOccurrences) {
		this.pairCount = pairCount;
		this.wordOccurrences = WordOccurrences;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		pairCount.write(out);
		wordOccurrences.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		pairCount.readFields(in);
		wordOccurrences.readFields(in);
		
	}

	@Override
	public int compareTo(FirstJobValue o) {
		int wordCompare = this.pairCount.compareTo(o.getpairCount());
		if( wordCompare == 0) {
			return this.wordOccurrences.compareTo(o.getWordOccurrences());
		}
		return wordCompare;
	}
	
	public LongWritable getpairCount() {
		return pairCount;
	}
	
	public LongWritable getWordOccurrences() {
		return wordOccurrences;
	}
	
    public String toString() {
        return this.pairCount.toString() + " " + this.wordOccurrences.toString();
    }
}
