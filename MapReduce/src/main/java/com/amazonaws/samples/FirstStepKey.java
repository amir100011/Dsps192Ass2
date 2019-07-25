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
	
	public FirstStepKey(Text otherFirstWord, Text otherSecondWord, IntWritable otherDecade) {
		this.firstWord = new Text(otherFirstWord.toString());
		this.secondWord = new Text(otherSecondWord.toString());
		this.decade = new IntWritable(otherDecade.get());		
	}
	
	public FirstStepKey(String otherFirstWord, String otherSecondWord, IntWritable otherDecade) {
		this.firstWord = new Text(otherFirstWord);
		this.secondWord = new Text(otherSecondWord);
		this.decade = new IntWritable(otherDecade.get());		
	}

	public void setFields(String otherFirstWord, String otherSecondWord, int otherDecade) {
		this.firstWord.set(otherFirstWord);
		this.secondWord.set(otherSecondWord);
		this.decade.set(otherDecade);		
	}

	public FirstStepKey( FirstStepKey otherFirstKey) {
		this.firstWord = otherFirstKey.getFirstWord();
		this.secondWord = otherFirstKey.getSecondWord();
		this.decade = otherFirstKey.getDecade();		
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
	public int compareTo(FirstStepKey otherFirstStepKey) {
		int decadeCompared = this.decade.compareTo(otherFirstStepKey.getDecade());
		if (decadeCompared != 0)
			return decadeCompared;
		else //same decade
		{
			if(!this.firstWord.toString().equals("*") && otherFirstStepKey.getFirstWord().toString().equals("*")){
				return 1;				
			}else if(this.firstWord.toString().equals("*") && !otherFirstStepKey.getFirstWord().toString().equals("*")){
				return -1;				
			}else {
				int firstwordcompare = this.firstWord.compareTo(otherFirstStepKey.getFirstWord());
				if(firstwordcompare == 0) {
					if(this.secondWord.toString().equals("*") && !otherFirstStepKey.getSecondWord().toString().equals("*"))
						return -1;
					else if(!this.secondWord.toString().equals("*") && otherFirstStepKey.getSecondWord().toString().equals("*"))
						return 1;
					else {
						return this.secondWord.compareTo(otherFirstStepKey.getSecondWord());
					}					
				}else
					return firstwordcompare;				
			}
		}
	}

	public String toString() {
		return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.decade.toString();
	}

	public int getCode() {
		return firstWord.hashCode() + decade.hashCode();
	}
}