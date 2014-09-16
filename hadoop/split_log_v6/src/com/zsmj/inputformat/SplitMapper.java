package com.zsmj.inputformat;

import java.util.HashSet;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	// url过滤黑名单
	protected static HashSet<String> EXCLUDE_FTYPE = new HashSet<String>();
	protected static HashSet<String> EXCLUDE_URI = new HashSet<String>();
	static {
		EXCLUDE_FTYPE.add("js");
		EXCLUDE_FTYPE.add("css");
		EXCLUDE_FTYPE.add("png");
		EXCLUDE_FTYPE.add("gif");
		EXCLUDE_FTYPE.add("jpg");
		EXCLUDE_FTYPE.add("jpeg");
		EXCLUDE_URI.add("/zybook/u/p/sysini.php");
		EXCLUDE_URI.add("/zybook/u/p/section.php");
		//EXCLUDE_URI.add("/zybook/u/p/api.php");
		EXCLUDE_URI.add("/zybook/u/p/cps.php");
	}
	// map函数
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String jobtype = context.getJobName().split(":")[0];
        String result = this.process(value.toString(),jobtype,context);
        if(null != result){
            context.write(key,new Text(result));
        }
    }
	// 判断是否是有效url
	private boolean isvalid(String url){
		boolean valid = true;
		String [] arr = url.split("\\?");
		if (arr.length > 0){
			String uri = arr[0];
			if (EXCLUDE_URI.contains(uri))
				valid = false;
			else{
				String [] ft_arr = uri.split("\\.");
				if (ft_arr.length >= 2 && EXCLUDE_FTYPE.contains(ft_arr[ft_arr.length-1]))
					valid = false;
			}
		}
		return valid;
	}
	// 解析有效的url，生成需要的中间结果
    private String process(String line, String jobtype, Context context){
    	String result = null;
    	if(null != line) {
	    	try{
		    	String[] info = line.replaceAll("\t","").split(" ");
		    	if (info.length >= 10) {		    		
					String ip = info[0];
					String datetime = info[3];
					String url = info[6];
					int status =  Integer.parseInt(info[8]);
					datetime = Util.strftime(Util.strptime(datetime.substring(1),"dd/MMM/yyyy:HH:mm:ss"),"yyyy-MM-dd HH:mm:ss");
					if (status>=200 && status<=300){
				    	if (jobtype.equals("download_v6")){
							if (url.indexOf("/r/download") >= 0){
								result = SplitParser.parse(jobtype,url,datetime,ip);
							}
				    	}
				    	else if (jobtype.equals("service_v6")){
				    		if (isvalid(url))
								result = SplitParser.parse(jobtype,url,datetime,ip);
				    	}
				    	context.getCounter("RecordsInfo","correct").increment(1);
					}
					else
						context.getCounter("RecordsInfo","httpfailed").increment(1);
		    	}
		    	else
		    		context.getCounter("RecordsInfo","malformed").increment(1);
	    	}catch(Throwable e){
	    		context.getCounter("RecordsInfo","malformed").increment(1);
	    	}
    	}
    	return result;
    }
}
