package com.zsmj.inputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVisCombineMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	private static int csvlen=10;
	private String[] split(String record) {

		try {

			if (record == null) {
				new Throwable("null input Log line");
			}
			String[] items = record.split("\t", -1);
			if (items.length == csvlen ) {

				return new String[] { items[2],record};

			} else {
				new Throwable("bad fields number int Log line");
				return null;
			}
		} catch (Throwable e) {
			e.printStackTrace();
			return null;
		}
	}

}
