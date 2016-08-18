package com.hadoop.log.count;

import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(final String[] args) {
		
		/**
		 * Job One
		 */
		try {
			ToolRunner.run(new StatusCount(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/**
		 * Job Two
		 */
		try {
			ToolRunner.run(new PartByIp(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/**
		 * Job Three
		 */
		try {
			ToolRunner.run(new URLCount(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/**
		 * Job Four
		 */
		try {
			ToolRunner.run(new URLAveCount(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
