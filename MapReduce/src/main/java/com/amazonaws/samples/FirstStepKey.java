package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class FirstStepKey implements WritableComparable<FirstStepKey> {
	private Text firstWord;
	private Text secondWord;
	private IntWritable decade;
	
	public FirstStepKey() {
		this.firstWord = new Text();
		this.secondWord = new Text();
		this.decade = new IntWritable();		
	}
	
	public void setFields(String otherFirstWord, String otherSecondWord, int otherDecade) {
		this.firstWord.set(otherFirstWord);
		this.secondWord.set(otherSecondWord);
		this.decade.set(otherDecade);		
	}
	
	public FirstStepKey( FirstStepKey otherFirstKey) {
		this.firstWord = otherFirstKey.getFirstWord();
		this.secondWord = otherFirstKey.getSecondWord();
		this.decade = otherFirstKey.getYear();		
	}
	
	public Text getFirstWord() {
		return this.firstWord;
	}
	
	public Text getSecondWord() {
		return this.secondWord;
	}
	
	public IntWritable getYear() {
		return this.decade;
	}
	
	public void readFields(DataInput in) throws IOException {
		((Writable) firstWord).readFields(in) ;
		((Writable) secondWord).readFields(in) ;
		decade.readFields(in) ;
		
	}

	public void write(DataOutput out) throws IOException {
		((Writable) firstWord).write(out) ;
		((Writable) secondWord).write(out) ;
		decade.write(out) ;
		
	}

	public int compareTo(FirstStepKey arg0) {
		
		return 0;
	}
	
	public String toString() {
        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.decade.toString();
    }
	
	public int getCode() {
		return firstWord.hashCode() + decade.hashCode();
	}
	
}
