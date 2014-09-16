package com.zsmj.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	
	public static void main(String[] args) throws Exception {
		try{
		    Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    Job job = null;
		    job = new Job(conf);
		    job.setJobName(otherArgs[2]); // format: download_v6:20121024:10  service_v6:20121024:10
		    job.setJarByClass(Driver.class);
		    job.setMapperClass(SplitMapper.class);
		    job.setNumReduceTasks(1);
		    job.setOutputValueClass(Text.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputFormatClass(SplitOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    SplitOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    if(!job.waitForCompletion(true)){
		          Email.defaultSend("UTF-8","split log v6 exception","split log v6 exception");
	    }
		}catch(Throwable e){
			Email.defaultSend("UTF-8","split log v6 exception",e.getMessage());
		}
	 }
}
