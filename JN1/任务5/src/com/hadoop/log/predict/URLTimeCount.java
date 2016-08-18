package com.hadoop.log.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class URLTimeCount  implements Writable{
	
	private Text url;
	private Text time;
	private LongWritable count;
	
	public URLTimeCount(){
		this.url = new Text();
		this.time = new Text();
		this.count = new LongWritable();
	}
	
	public URLTimeCount(String url, String time, long count) {
		super();
		this.url = new Text(url);
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
	
	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		url.readFields(arg0);
		time.readFields(arg0);
		count.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		url.write(arg0);
		time.write(arg0);
		count.write(arg0);
	}
	
}
