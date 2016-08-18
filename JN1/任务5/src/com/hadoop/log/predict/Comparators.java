package com.hadoop.log.predict;

import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Comparators {
	
	public static class GroupingComparator extends WritableComparator{
		
		public GroupingComparator(){
			super(URLInterface.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return ((URLInterface) a).getUrl().compareTo(((URLInterface) b).getUrl());
		}
		
	}
	
	public static class GroupingComparatorForPredict extends WritableComparator{
		
		public GroupingComparatorForPredict(){
			super(URLInterface.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			URLInterface url1 = (URLInterface) a;
			URLInterface url2 = (URLInterface) b;
			Date date1 = null;
			Date date2 = null;
			try {
				date1 = Tools.parseToDate2(url1.getTime());
				date2 = Tools.parseToDate2(url2.getTime());
				date1.setYear(2000);
				date1.setMonth(12);
				date1.setDate(1);
				date2.setYear(2000);
				date2.setMonth(12);
				date2.setDate(1);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				return 0;
			}
			return date1.compareTo(date2);
		}
		
	}
	
	public static class SortComparatorForPredict extends WritableComparator{
		
		public SortComparatorForPredict(){
			super(URLInterface.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			URLInterface url1 = (URLInterface) a;
			URLInterface url2 = (URLInterface) b;
			Date date1 = null;
			Date date2 = null;
			try {
				date1 = Tools.parseToDate2(url1.getTime());
				date2 = Tools.parseToDate2(url2.getTime());
				date1.setYear(2000);
				date1.setMonth(12);
				date1.setDate(1);
				date2.setYear(2000);
				date2.setMonth(12);
				date2.setDate(1);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				return 0;
			}
			int cmp = date1.compareTo(date2);
			if(cmp != 0)
				return cmp;
			cmp = url1.getUrl().compareTo(url2.getUrl());
			if(cmp != 0)
				return cmp;
			try {
				date1 = Tools.parseToDate2(url1.getTime());
				date2 = Tools.parseToDate2(url2.getTime());
				date1.setYear(2000);
				date1.setMonth(12);
				date2.setYear(2000);
				date2.setMonth(12);
				date1.setHours(0);
				date2.setHours(0);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				return 0;
			}
			return date1.compareTo(date2);
		}
		
	}
	
	
}
