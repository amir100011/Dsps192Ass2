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
	
	public FirstStepKey(String otherFirstWord, String otherSecondWord, int otherDecade) {
		this.firstWord = new Text(otherFirstWord);
		this.secondWord = new Text(otherSecondWord);
		this.decade = new IntWritable(otherDecade);		
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

	public int compareTo(FirstStepKey otherFirstStepKey) {
		int decadeCompared = this.decade.compareTo(otherFirstStepKey.getDecade());
		if (decadeCompared != 0)
			return decadeCompared;
		else //same decade
		{
			if(this.firstWord.toString() == "*" || otherFirstStepKey.getFirstWord().toString() == "*") 
			{
				if(this.firstWord.toString() != "*")
					return -1;
				else if(otherFirstStepKey.getFirstWord().toString() != "*")
					return 1;
				else
					return this.secondWord.compareTo(otherFirstStepKey.getSecondWord());
			} else{	
				int firstWordCompared = this.firstWord.compareTo(otherFirstStepKey.getFirstWord());
				if(this.secondWord.toString() == "*") 
				{
					if (otherFirstStepKey.getSecondWord().toString() != "*")
						return firstWordCompared == 0 ? -1:firstWordCompared;
					else
						return -firstWordCompared;		
				} else{//this.secondWord != "*"
					if (otherFirstStepKey.getSecondWord().toString() == "*")
						return firstWordCompared == 0 ? 1:firstWordCompared;
					else
					{
						int secondWordCompared = this.secondWord.compareTo(otherFirstStepKey.getSecondWord());
						return firstWordCompared == 0 ? secondWordCompared:firstWordCompared;	
					}
				}
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
