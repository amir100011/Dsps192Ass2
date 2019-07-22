package com.amazonaws.samples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FirstJobKey implements WritableComparable<FirstJobKey>{

	private Text firstWord;
	private Text secondWord;
	private IntWritable decade;
	
	public FirstJobKey() {
		firstWord = new Text();
		secondWord = new Text();
		decade = new IntWritable();
	}
	
	public FirstJobKey(Text firstWord, Text secondWord,IntWritable decade) {
		this.firstWord = firstWord;
		this.secondWord = secondWord;
		this.decade = decade;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		firstWord.write(out);
		secondWord.write(out);
		decade.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstWord.readFields(in);
		secondWord.readFields(in);
		decade.readFields(in);
	}

	@Override
	public int compareTo(FirstJobKey o) {
		int decadeCompare = this.decade.compareTo(o.getDecade());
		if(decadeCompare == 0) {
		//Compare by first word first
		Boolean firstWordAsterisk = this.firstWord.toString().equals("*");
		Boolean otherFirstWordAsterisk = o.getFirstWord().toString().equals("*");
		if(firstWordAsterisk && !otherFirstWordAsterisk) {
			//first word is *, other first word is Not *
			return -1;
		} else if(otherFirstWordAsterisk && !firstWordAsterisk){
			//first word is Not *, other first word is *
			return 1;
		} else {
			//Both Asterisk, or both not Asterisk 
			int firstWordComapre = this.firstWord.compareTo(o.getFirstWord());
			if( firstWordComapre == 0) {
				//Compare by second word 
				Boolean secondWordAsterisk = this.secondWord.toString().equals("*");
				Boolean otherSecondWordAsterisk = o.getSecondWord().toString().equals("*");
				if(secondWordAsterisk && !otherSecondWordAsterisk) {
					//second word is *, other first word is Not *
					return -1;
				} else if(otherSecondWordAsterisk && !secondWordAsterisk){
					//first word is Not *, other first word is *
					return 1;
				} else {
				return this.secondWord.compareTo(o.getSecondWord());
				}
			}
			return firstWordComapre;
		}
		} else {
			return decadeCompare;
		}
	}
	
	public int getCode() {
		return firstWord.hashCode() + decade.hashCode();
	}
	
	public Text getFirstWord() {
		return firstWord;
	}
	
	public Text getSecondWord() {
		return secondWord;
	}
	
	public IntWritable getDecade() {
		return decade;
	}

	@Override
    public String toString() {
        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.decade.toString();
    }

}
