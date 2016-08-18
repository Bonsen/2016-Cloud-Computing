package com.hadoop.log.count;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StatusInterface implements WritableComparable<StatusInterface>{
	
	private IntWritable timeInt;
	private LongWritable status_200;
	private LongWritable status_404;
	private LongWritable status_500;
	
	public StatusInterface(){
		this.timeInt = new IntWritable(-1);
		this.status_200 = new LongWritable(0);
		this.status_404 = new LongWritable(0);
		this.status_500 = new LongWritable(0);
	}
	
	public StatusInterface(Text status, IntWritable timeInt){
		super();
		this.timeInt = timeInt;
		this.status_200 = new LongWritable(0);
		this.status_404 = new LongWritable(0);
		this.status_500 = new LongWritable(0);
		if(status.toString().equals("200")){
			this.status_200 = new LongWritable(1);
		}
		if(status.toString().equals("404")){
			this.status_404 = new LongWritable(1);
		}
		if(status.toString().equals("500")){
			this.status_500 = new LongWritable(1);
		}
	}
	
	public int getTimeInt(){
		return this.timeInt.get();
	}
	
    public void StatusSum(StatusInterface t){
  	  this.timeInt = new IntWritable(t.getTimeInt());
  	  this.status_200 = new LongWritable(t.status_200.get() + this.status_200.get());
  	  this.status_404 = new LongWritable(t.status_404.get() + this.status_404.get());
  	  this.status_500 = new LongWritable(t.status_500.get() + this.status_500.get());
    }
    
    public void clear(){
    	  this.timeInt = new IntWritable(-1);
    	  this.status_200 = new LongWritable(0);
    	  this.status_404 = new LongWritable(0);
    	  this.status_500 = new LongWritable(0);
    }
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		timeInt.readFields(in);
		this.status_200.readFields(in);
		this.status_404.readFields(in);
		this.status_500.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		timeInt.write(out);
		status_200.write(out);
		status_404.write(out);
		status_500.write(out);
	}

	@Override
	public int compareTo(StatusInterface o) {
		// TODO Auto-generated method stub
		
		int cmp;
		if(o==null)
			cmp = -1;
		else
			cmp = this.timeInt.compareTo(o.timeInt);
		return cmp;
	}
	
	public String toString1(){
		return "200:" + status_200 + " 404:" + status_404 + " 500:" + status_500; 
	}
	
	public String toString2(){
		return "200:" + status_200 + "\n404:" + status_404 + "\n500:" + status_500;
	}

}
