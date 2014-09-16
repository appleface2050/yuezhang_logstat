package com.zsmj.inputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserInitCombineMapper extends Mapper<LongWritable, Text, Text, Text> {
	// p1,p2,p4,p7,user,p16,p19,init_time

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] result = split(value.toString());
		// String result =value.toString();

		if (result != null)
			if (null != result[0] && null != result[1]) {
				context.write(new Text(result[0]), new Text(result[1]));
			}
	}

	private String[] split(String record) {

		try {

			if (record == null) {
				new Throwable("null input Log line");
			}
			String[] items = record.split("\t", -1);
			if (items.length == 9 ) {

				return new String[] { items[2],record};

			}else if(items.length == 10 && (items[9]==null || items[9].equals(""))){ 
				return new String[] { items[2],record};
			}	 else {
				new Throwable("bad fields number int Log line");
				return null;
			}
		} catch (Throwable e) {
			e.printStackTrace();
			return null;
		}
	}

}
