package com.zsmj.inputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p15,p16,p19,p21,first_time,second_time,
 * cureent_time 17个字段
 * 
 * @author ZSMJ
 * 
 */
public class UserVisReducer extends Reducer<Text, Text, Text, Text> {

	List<String> buf = new ArrayList<String>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		String record = null;
		while (it.hasNext()) {
			record = it.next().toString();			
			if (buf.isEmpty()){
				buf.add(record);
				buf.add(record);
			}
			if(record.compareTo(buf.get(0))<0)
				buf.set(0,record);
			if(record.compareTo(buf.get(1))>0)
				buf.set(1, record);
		}

		if (!buf.isEmpty()) {
			String s1=buf.get(0);
			String[] s2=buf.get(1).split("\t");
			context.write(key,new Text(s2[0]+"\t"+s1+"\t"+"v"));
		}
		buf.clear();
	}

}
