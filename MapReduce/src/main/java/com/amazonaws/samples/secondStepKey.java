package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
		npmi.readFields(in) ;

	}
	@Override
	public void write(DataOutput out) throws IOException {
		((Writable) firstWord).write(out) ;
		((Writable) secondWord).write(out) ;
		decade.write(out) ;
		npmi.write(out) ;
		}

	@Override
	public int compareTo(secondStepKey other) {
		//-1 --> my decade is smaller I will be first, 0 - equal, 1 - my decade is bigger I will be later on

		
		//increasing
		int decadeCompare = checkDecade(this.decade,other.decade); 
		if(decadeCompare != 0)
			return decadeCompare;
		
		boolean doubleStar = doubleStarCheck(this, other);
		if (doubleStar)
			return 0;
		
		boolean hasaStar = starCheck(this, other);
		if (hasaStar)
			return starOrder(this,other);
		
		//decending
		int npmiComapre = checkNPMI(this.npmi, other.getNpmi());
		if(npmiComapre != 0)
			return -npmiComapre;
		
		return (checkWords(this, other));
	}
	
	public int checkDecade(IntWritable mine, IntWritable other) {
		return mine.get() == other.get() ? 0:(mine.get() < other.get() ? -1:1); 
	}
	
	public boolean doubleStarCheck(secondStepKey mine, secondStepKey other) {
		return mine.getFirstWord().toString().equals("*") &&  other.getFirstWord().toString().equals("*");

	}
	
	public boolean starCheck(secondStepKey mine, secondStepKey other) {
		return mine.getFirstWord().toString().equals("*") ||  other.getFirstWord().toString().equals("*");

	}
	
	public int starOrder(secondStepKey mine, secondStepKey other) {
		return (!mine.getFirstWord().toString().equals("*") && other.getFirstWord().toString().equals("*")) ? 1:-1;
	}
	
	public int checkNPMI(DoubleWritable mine, DoubleWritable other) {
		return mine.get() == other.get() ? 0:(mine.get() < other.get() ? -1:1); 
	}
	
	public int checkWords(secondStepKey mine, secondStepKey other) {
		int firstWordComparison = mine.getFirstWord().toString().compareTo(other.getFirstWord().toString());
		if(firstWordComparison != 0)
			return firstWordComparison;
		return mine.getSecondWord().toString().compareTo(other.getSecondWord().toString());
	}
	
	public String toString() {
		return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.decade.toString() + " " + this.npmi.toString();
	}

	public int getCode() {
		return decade.hashCode();
	}

}
