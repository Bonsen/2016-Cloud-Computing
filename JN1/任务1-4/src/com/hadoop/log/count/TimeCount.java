package com.hadoop.log.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TimeCount  implements Writable{
	
	private Text time;
	private LongWritable count;
	
	public TimeCount(){
		this.time = new Text();
		this.count = new LongWritable();
	}
	
	public TimeCount(String time, long count) {
		super();
		this.time = new Text(time);
		this.count = new LongWritable(count);
	}

	public Text getTime() {
		return time;
	}

	public void setTime(Text time) {
		this.time = time;
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		time.readFields(arg0);
		count.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		time.write(arg0);
		count.write(arg0);
	}
	
}
