package com.hadoop.log.count;
import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IpTime implements WritableComparable<IpTime>{
	
	private Text Ip;
	private IntWritable timeInt;
	
	public IpTime(){
		this.Ip = new Text();
		this.timeInt = new IntWritable(-1);
	}
	
	public IpTime(Text Ip, IntWritable timeInt){
		this.Ip = Ip;
		this.timeInt = timeInt;
	}
	
	public int getTimeInt(){
		return this.timeInt.get();
	}
	
	public Text getIp(){
		return this.Ip;
	}
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		timeInt.readFields(in);
		Ip.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		timeInt.write(out);
		Ip.write(out);
	}

	@Override
	public int compareTo(IpTime o) {
		// TODO Auto-generated method stub
		
		int cmp;
		if(o==null)
			cmp = -1;
		else 
			cmp = this.Ip.compareTo(o.getIp());
		return cmp;
	}
	
	public String toString(){
		return this.Ip.toString() +" " +  this.timeInt;  
	}

}
