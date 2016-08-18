package com.hadoop.log.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class URLInterface implements WritableComparable<URLInterface> {
	
	private Text url;
	private Text time;
	
	public URLInterface() {
		this.url = new Text();
		this.time = new Text();
	}
	
	public URLInterface(String url, String time) {
		super();
		this.url = new Text(url);
		this.time = new Text(time);
	}

	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	public Text getTime() {
		return time;
	}

	public void setTime(Text time) {
		this.time = time;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException{
		url.readFields(dataInput);
		time.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		url.write(dataOutput);
		time.write(dataOutput);
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.url.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj != null && obj instanceof URLInterface)
			return this.url.equals(((URLInterface) obj).getUrl()) && this.time.equals(((URLInterface) obj).getTime());
		return false;
	}

	@Override
	public String toString() {
		try {
			return url + "\t" + Tools.parseToString(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int compareTo(URLInterface o){
		if(o == null)
			return -1;
		int cmp = this.url.compareTo(o.getUrl());
		if(cmp != 0)
			return cmp;
		try {
			cmp = Tools.compareDate(this.time, o.getTime());
		} catch (ParseException e) {
			return -1;
		}
		return cmp;
	}

}
