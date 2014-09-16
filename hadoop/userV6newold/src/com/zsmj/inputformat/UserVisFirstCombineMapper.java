package com.zsmj.inputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVisFirstCombineMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text uid = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] result = split(value.toString());
		// String result =value.toString();

		if (result != null)
			if (null != result[0] && null != result[1]) {
				context.write(new Text(result[0]), new Text(result[1]));
			}
	}
	private static final int csvlen=9;
	private String[] split(String record) {

		try {

			if (record == null) {
				new Throwable("null input Log line");
			}
			String[] items = record.split("\u0001", -1);
			if (items.length == csvlen ) {

				return new String[] { items[0],record};

			}else if(items.length == csvlen+1 && (items[csvlen]==null || items[csvlen].equals(""))){ 
				return new String[] { items[0],record};
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
