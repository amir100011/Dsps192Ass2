package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class secondStepKey implements WritableComparable<secondStepKey> {
	
	private Text firstWord;
	private Text secondWord;
	private IntWritable decade;
	private DoubleWritable npmi;
	
	public secondStepKey() {
		  this.firstWord = new Text();
		  this.secondWord = new Text();
		  this.decade = new IntWritable(0);
		  this.npmi = new DoubleWritable(0);
	}
	
	public secondStepKey(String otherFirstWord, String otherSecondWord, int dec,double nmpi) {
		  this.firstWord = new Text(otherFirstWord);
		  this.secondWord = new Text(otherSecondWord);
		  this.decade = new IntWritable(dec);
		  this.npmi = new DoubleWritable(nmpi);
	}
	
	public secondStepKey(Text fw, Text sw, IntWritable dec,DoubleWritable nmpi ) {
		this.firstWord = new Text(fw.toString());
		  this.secondWord = new Text(sw.toString());
		  this.decade = new IntWritable(dec.get());
		  this.npmi = new DoubleWritable(nmpi.get());
	}	
	
	public void SetsecondStepKeyValues(Text fw, Text sw, IntWritable dec,DoubleWritable nmpi ) {
		  this.firstWord =fw;
		  this.secondWord =sw;
		  this.decade =dec;
		  this.npmi = nmpi;
	}	
	
	public Text getFirstWord() {
		return this.firstWord;
	}
	
	public Text getSecondWord() {
		return this.secondWord;
	}
	
	public IntWritable getDecade() {
		return this.decade;
	}
	
	public DoubleWritable getNpmi() {
		return this.npmi;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) firstWord).readFields(in) ;
		((Writable) secondWord).readFields(in) ;
		decade.readFields(in) ;

	}
	@Override
	public void write(DataOutput out) throws IOException {
		((Writable) firstWord).write(out) ;
		((Writable) secondWord).write(out) ;
		decade.write(out) ;
		}

	@Override
	public int compareTo(secondStepKey other) {
		int decadeCompare = this.decade.compareTo(other.getDecade());
		if(decadeCompare == 0) {
			if(this.firstWord.toString().equals("*") && !other.getFirstWord().toString().equals("*")) 
				return -1;
			else if(!this.firstWord.toString().equals("*") && other.getFirstWord().toString().equals("*"))
				return 1;				
			else {
				int npmiCompare = this.npmi.compareTo(other.getNpmi());
				if(npmiCompare == 0) {
					int firstWordCompare = this.firstWord.compareTo(other.getFirstWord());
					if(firstWordCompare == 0)
						return this.secondWord.compareTo(other.getSecondWord());
					return firstWordCompare;
				}
				return npmiCompare;
			}
		}
		return decadeCompare;		
	}
	
	public String toString() {
		return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.decade.toString() + " " + this.npmi.toString();
	}

	public int getCode() {
		return decade.hashCode();
	}

}
