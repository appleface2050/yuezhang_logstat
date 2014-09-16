package com.zsmj.inputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * cureent_time 10个字段
 * 
 * @author ZSMJ
 * 
 */
public class UserVisCombineReducer extends Reducer<Text, Text, Text, Text> {


	private static final int csvlen=10;
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] buf = new String[] { "", "", "", "", "", "", "", "" ,"","v"};
		Iterator<Text> it = values.iterator();
		String record = null;
		while (it.hasNext()) {
			record = it.next().toString();

			String[] items = record.split("\t", -1);
			if(buf[0].equals("")) System.arraycopy(items, 0, buf, 0, csvlen-1);
			String last_init_time = items[0];
			String first_init_time = items[1];

			if (first_init_time.compareTo(buf[1]) < 0){
				System.arraycopy(items, 1, buf, 1, csvlen - 2);
				//buf[8]="good";
			}
				

			if (last_init_time.compareTo(buf[0]) > 0){
				buf[0] = last_init_time;
			}

		}
		String txt=buf[0];
		for(int i=1;i<csvlen-1;i++){
			txt +="\t"+buf[i];
		}
		txt+="\t"+buf[csvlen-1];
		context.write(key, new Text(txt));

	}

}
