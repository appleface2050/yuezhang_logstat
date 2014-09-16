package com.zsmj.inputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVisMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text uid = new Text();
	private static Pattern p2pattern = Pattern.compile("^(108|109|110|111)");
	private static Pattern keypattern = Pattern.compile("^(4B4|1U1|1R1|1P1|1K1|17B|12[689]B)");
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
			if (items.length == 29) {
				Matcher mat1= p2pattern.matcher( items[1]);
				Matcher mat2= keypattern.matcher( items[18]);
				if( mat1.find() && mat2.find())
					return new String[] { items[0], items[28]+"\t"+items[0]+"\t"+items[1]+"\t"+items[3]+"\t"+items[6]+"\t"+items[16]+"\t"+items[11]+"\t"+items[12] };
				else
					return null;

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
