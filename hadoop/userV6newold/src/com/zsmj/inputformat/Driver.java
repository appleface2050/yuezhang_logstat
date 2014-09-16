package com.zsmj.inputformat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Driver {

	

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    conf.set("mapred.reduce.max.attempts","1");
	    Job job = null;
	    try{	    		
	    	    job = new Job(conf, "Combine newuser & olduser:"+otherArgs[2]);
	            job.setJarByClass(Driver.class);
	    	    String mrType = otherArgs[2].split(":")[2];
	    	    MREnum mrenum = MREnum.valueOf(mrType.toUpperCase());
	            switch(mrenum){
	            	case 	USERINIT:
	    	            job.setMapperClass(UserInitSplitMapper.class);
	    	            job.setReducerClass(UserInitReducer.class);	            		
	            		break;
	            	case USERINITCOMB:
	    	            job.setMapperClass(UserInitCombineMapper.class);
	    	            job.setReducerClass(UserInitCombineReducer.class);            		
	            		break;
	            	case USERVIS:
	    	            job.setMapperClass(UserVisMapper.class);
	    	            job.setReducerClass(UserVisReducer.class);
	            		break;
	            	case USERVISCOMB:
	    	            job.setMapperClass(UserVisCombineMapper.class);
	    	            job.setReducerClass(UserVisCombineReducer.class);
	            		break;
	            	case VISJOIN:
	    	            job.setMapperClass(UserVisJoinMapper.class);
	    	            job.setReducerClass(UserVisJoinReducer.class);	 
	            		break;
	            	case FORMATCLEAR:
	            		break;
	            }
	            job.setMapOutputValueClass(Text.class);
	            job.setMapOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);
	            job.setOutputKeyClass(Text.class);
	            job.setOutputFormatClass(SplitOutputFormat.class);
	            job.setNumReduceTasks(1);
	            for (String inpstr : otherArgs[0].split(",")){
	            	FileInputFormat.addInputPath(job, new Path(inpstr));
	            }	            
	            SplitOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	            if(!job.waitForCompletion(true)){
	            	//Email.defaultSend("UTF-8","掌阅日志切分异常报告","V5services");
	            }
	    	}catch(Throwable e){
	    		e.printStackTrace();
	    		//Email.defaultSend("UTF-8","掌阅日志切分异常报告","V5services");
	    	}
	}
}
