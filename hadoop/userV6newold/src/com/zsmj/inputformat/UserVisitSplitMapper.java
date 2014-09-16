package com.zsmj.inputformat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserVisitSplitMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static Set<String> fieldset=new HashSet<String>();
	static {
		fieldset.add("p1");
		fieldset.add("p2");
		fieldset.add("p4");
		fieldset.add("p7");
		fieldset.add("user");
		fieldset.add("p16");
		fieldset.add("p19");		
	}

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
			if (items.length == 5) {
				String urlParam = items[3].substring(items[3].indexOf('?') + 1);
				String[] params = urlParam.split(",", -1);
				Map<String, String> dataMap = new HashMap<String, String>();
				for (String param : params) {
					String[] t = param.split("=");
					if (t.length == 2) {
						if (t[0].equals("key") && !t[1].equals("1T1"))
							break;
						if (!fieldset.contains(t[0]))
							continue;
						if ("".equals(t[1])) {
							dataMap.put(t[0], "null");
						} else {
							dataMap.put(t[0], t[1]);
						}
					}
				}
				String vtime = items[1].substring(0, items[1].length() - 4);
				dataMap.put("init_time", vtime);

				UserInitRecord r = new UserInitRecord(dataMap);
				return new String[] { dataMap.get("p1"), r.toString() };

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
