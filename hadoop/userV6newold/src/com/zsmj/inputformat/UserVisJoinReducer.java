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
public class UserVisJoinReducer extends Reducer<Text, Text, Text, Text> {

	private static final int icsvlen=9;
	private static final int vcsvlen=10;
	private static final int cmbcsvlen=11;
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] buf = new String[] { "null", "null", "null", "null", "null", "null", "null", "null" ,"null","null","null"};
		Iterator<Text> it = values.iterator();
		String record = null;
		while (it.hasNext()) {
			record = it.next().toString();
			
			String[] items = record.split("\t", -1);
			if (items.length==icsvlen){
				buf[7]=items[1];
				buf[8]=items[0];
				 System.arraycopy(items, 2, buf, 0, 4);
				 System.arraycopy(items, 2+4, buf, 5, 2);				 
			}
			if (items.length==vcsvlen && buf[0].equals("null")){
				buf[9]=items[1];
				buf[10]=items[0];
				 System.arraycopy(items,2,buf,0,vcsvlen-2-1);
			}else if(items.length==vcsvlen && !buf[0].equals("null")){
				buf[9]=items[1];
				buf[10]=items[0];				 
			}			

		}
		if(buf[7].compareTo(buf[9])>0)
			buf[7]=buf[9];
		
		String txt=buf[0];
		for(int i=1;i<cmbcsvlen;i++)
			txt+="\t"+buf[i];
		context.write(key, new Text(txt));

	}

}
