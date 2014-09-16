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
public class UserVisFirstCombineReducer extends Reducer<Text, Text, Text, Text> {


	private static final int csvlen=9;
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] buf = new String[] { "", "", "", "", "", "", "", "","v"};
		Iterator<Text> it = values.iterator();
		String record = null;
		while (it.hasNext()) {
			record = it.next().toString();

			String[] items = record.split("\u0001", -1);
			if(buf[0].equals("")) {
				System.arraycopy(items, 0, buf, 0, csvlen);
			}
			String last_init_time = items[csvlen-1];
			String first_init_time = items[csvlen-2];

			if (first_init_time.compareTo(buf[csvlen-2]) < 0){
				System.arraycopy(items, 1, buf, 1, csvlen - 1);
			}				

			if (last_init_time.compareTo(buf[csvlen-1]) > 0){
				buf[csvlen-1] = last_init_time;
			}

		}
		String txt=buf[csvlen-1]+"\t"+buf[csvlen-2];
		for(int i=0;i<csvlen-2;i++){
			txt +="\t"+buf[i];
		}
		//txt+="\t"+buf[csvlen];
		context.write(key, new Text(txt));

	}

}
