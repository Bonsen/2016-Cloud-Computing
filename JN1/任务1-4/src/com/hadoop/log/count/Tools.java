package com.hadoop.log.count;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class Tools {
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", Locale.ENGLISH);
	private static SimpleDateFormat sdf0 = new SimpleDateFormat("dd/MMM/yyyy:HH", Locale.ENGLISH);
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");
	private static SimpleDateFormat sdf3 = new SimpleDateFormat("HH:mm");
	private static SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd   HH:mm");
	private static DecimalFormat df = new DecimalFormat("0.00000");
	
	public static int compareDate(Text t1, Text t2) throws ParseException{
		if(t1.toString().contains("+0800"))
			return sdf.parse(t1.toString()).compareTo(sdf.parse(t2.toString()));
		return sdf0.parse(t1.toString()).compareTo(sdf0.parse(t2.toString()));
	}
	
	public static Date parseToDate(Text time) throws ParseException{
		return sdf.parse(time.toString());
	}
	
	public static Date parseToDate2(Text time) throws ParseException{
		return sdf0.parse(time.toString());
	}
	
	public static String parseToString(Text time) throws ParseException{
		return sdf2.format(parseToDate(time));
	}
	
	public static String parseToString2(Text time) throws ParseException{
		return sdf3.format(parseToDate2(time));
	}
	
	public static String parseToString3(Text time) throws ParseException{
		return sdf4.format(parseToDate(time));
	}
	
	public static String parseToString4(Text time) throws ParseException{
		return sdf4.format(parseToDate2(time));
	}
	
	
	public static String parseDate(Date date){
		return sdf3.format(date);
	}
	
	public static String parseDate2(Date date){
		return sdf4.format(date);
	}
	
	public static String grabTime(Text text){
		String temp = text.toString();
		int start = temp.indexOf("[") + 1;
		int end = temp.indexOf("]");
		return temp.substring(start, end);
	}
	
	public static String grabHour(Text text){
		String temp = text.toString();
		int start = temp.indexOf("[") + 1;
		int end = temp.indexOf("]") - 12;
		return temp.substring(start, end);
	}
	
	public static double grabRespond(Text text) throws NumberFormatException {
		String temp = text.toString();
		return Double.valueOf(temp.substring(temp.lastIndexOf(" ") + 1, temp.length()));
	}
	
	public static String grabURL(Text text){
		String temp = text.toString();
		int start = temp.indexOf("\"") + 6;
		int end = temp.indexOf("HTTP") - 1;
		temp = temp.substring(start, end);
		return temp;
	}
	
	public static String format(double d){
		return df.format(d);
	}
	
	public static String predict(long[] original){
		double[] data = new double[original.length];
		for(int i =0; i < original.length; i++)
			data[i] = 0.0 + original[i];
		return format(PredictUtils.getPredictValue(data, original.length + 1));
	}
	
	public static void clearOriginal(long[] original){
		for(int i = 0; i < original.length; i++){
			original[i] = 0L;
		}
	}
	
	public static boolean checkDate(Date date){
		Date start = new Date(115, 8, 8);
		Date end = new Date(115, 8, 23);
		if(date.compareTo(start) >= 0 && date.compareTo(end) < 0)
			return true;
		return false;
	}
	
	public static void changeFileName(Path dir, Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FileStatus fileStatus = fs.getFileStatus(dir);
		if(fileStatus.isDirectory()){
			FileStatus[] files = fs.listStatus(dir);
			for (FileStatus file : files) {
				if(file.getPath().getName().contains(".txt-r-")){
					String newName = file.getPath().getName().substring(0, file.getPath().getName().indexOf(".txt-r-") + 4);
					fs.rename(file.getPath(), new Path(file.getPath().getParent(), newName));
				}
			}
		}
	}
}
