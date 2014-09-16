package com.zsmj.inputformat;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SplitOutputFormat<K, V> extends FileOutputFormat<K, V>{
	public final static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public final static String sql = "insert into sys_services_log_split_second values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public final static String mysqlDriverClass = "oracle.jdbc.driver.OracleDriver";
	public final static String mysqlURL = "jdbc:oracle:thin:@192.168.0.13:1521:informatica";
	public final static String mysqlUsername = "polaris_als";
	public final static String mysqlPassword = "";//zzhy1357";
	public final static int tryNumber = 5;
	protected static class SplitRecordWriter<K, V>extends RecordWriter<K, V> {
		private final static int max = 5000;
		private int count = 0;
		private String ETLTime;
		
		private PreparedStatement st;
		private DataOutputStream out;
		public SplitRecordWriter(DataOutputStream out,String ETLTime){
			this.out = out;
			
			this.ETLTime = ETLTime;
			
		}
		
		@Override
		public void close(TaskAttemptContext job) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			out.close();
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
		        Text to = (Text) value;
		        out.write(to.getBytes(), 0, to.getLength());
		        out.write("\n".getBytes("UTF-8"));
		      //批量写数据库

		}
		
		}
	
	@SuppressWarnings("deprecation")
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = job.getConfiguration();
		Path outDir = getOutputPath(job);
		FileSystem fs = outDir.getFileSystem(conf);
		String []param = job.getJobName().split(":",-1);
		Path date = new Path(outDir,new Path("ds="+param[1]));
		if(!fs.exists(date)){
			fs.mkdirs(date);
		}
		Path file = new Path(date,param[2]);
		FSDataOutputStream fileOut = null;
		fileOut = fs.create(file,true);
	    return new SplitRecordWriter<K, V>(fileOut,param[2]);
	}
	
	public void checkOutputSpecs(JobContext job) throws IOException{
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        
        if (outDir == null) {
             throw new InvalidJobConfException("Output directory not set.");
        }
    }
	

}
