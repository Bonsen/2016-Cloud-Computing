package com.hadoop.log.count;

public class myTime {
	public static String time2str(int time){
		String timeStart = String.format("%d", time);
		String timeEnd = String.format("%d", time + 1);
		return timeStart + ":00-" + timeEnd + ":00";
	}
	
}
