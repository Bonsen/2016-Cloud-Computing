package com.hadoop.log.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AveRespond implements Writable {
	
	private Text time;
	private LongWritable count;
	private DoubleWritable totalRespond;
	
	public AveRespond() {
		super();
		this.time = new Text();
		this.count = new LongWritable(1L);
		this.totalRespond = new DoubleWritable(0);
	}

	public AveRespond(String time, long count, double totalRespond) {
		super();
		this.time = new Text(time);
		this.count = new LongWritable(count);
		this.totalRespond = new DoubleWritable(totalRespond);
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

	public DoubleWritable getTotalRespond() {
		return totalRespond;
	}

	public void setTotalRespond(DoubleWritable totalRespond) {
		this.totalRespond = totalRespond;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.time.readFields(arg0);
		this.count.readFields(arg0);
		this.totalRespond.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.time.write(arg0);
		this.count.write(arg0);
		this.totalRespond.write(arg0);
	}

}
