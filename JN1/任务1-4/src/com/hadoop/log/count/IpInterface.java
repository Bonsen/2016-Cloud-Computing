package com.hadoop.log.count;
import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IpInterface implements WritableComparable<IpInterface>{
	
	private Text Ip;
	private LongWritable sum;
	private IntWritable timeInt;
	
	public IpInterface(){
		this.Ip = new Text();
		this.sum = new LongWritable(0);
		this.timeInt = new IntWritable(-1);
	}
	
	public IpInterface(IntWritable timeInt){
		super();
		this.sum = new LongWritable(0);
		this.timeInt = timeInt;
	}
	
	public IpInterface(Text Ip, IntWritable timeInt){
		super();
		this.Ip = Ip;
		this.sum = new LongWritable(1);
		this.timeInt = timeInt;
	}
	
	public int getTimeInt(){
		return this.timeInt.get();
	}
	
	public Text getIp(){
		return this.Ip;
	}
	
	public LongWritable getSum(){
		return this.sum;
	}
    public void IpSum(IpInterface t){
  	  this.timeInt = new IntWritable(t.getTimeInt());
  	  this.Ip = t.getIp();
  	  this.sum = new LongWritable(this.sum.get() + t.getSum().get());
    }
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		timeInt.readFields(in);
		sum.readFields(in);
		Ip.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		timeInt.write(out);
		sum.write(out);
		Ip.write(out);
	}

	@Override
	public int compareTo(IpInterface o) {
		// TODO Auto-generated method stub
		
		int cmp;
		if(o==null)
			cmp = -1;
		else
			cmp = this.Ip.compareTo(o.getIp());
		return cmp;
	}
	
	public String toString(){
		return sum.toString(); 
	}

}
