


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair>{

	private IntWritable year;
	private IntWritable temp;
	
	public IntWritable getyear() {
		return year;
	}
	
	public IntWritable gettemp() {
		return temp;
	}

	public IntPair() {
		this.year = new IntWritable(0);
		this.temp = new IntWritable(0);
	}
	
	public IntPair(int year, int temp) {
		this.year = new IntWritable(year);
		this.temp = new IntWritable(temp);
	}
	
	public void set(int year, int temp) {
		this.year.set(year);
		this.temp.set(temp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		year.readFields(in);
		temp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		year.write(out);
		temp.write(out);
	}

	@Override
	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		if (this.year.compareTo(o.year) == 0) {
			return (-1) * this.temp.compareTo(o.temp);
		}
		else {
			return this.year.compareTo(o.year);
		}
	}
	
	public String toString()
	{
		return year.toString() + "\t" + temp.toString();
	}
	
	/*
	@Override
	public int hashCode(){
		return year.hashCode();
	}
	*/
}
