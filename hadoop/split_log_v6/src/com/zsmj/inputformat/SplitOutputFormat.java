package com.zsmj.inputformat;
import java.io.DataOutputStream;
import java.io.IOException;
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
	public final static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected static class SplitRecordWriter<K, V> extends RecordWriter<K, V> {
		private DataOutputStream out;
		public SplitRecordWriter(DataOutputStream out){
			this.out = out;
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
		        Text to = (Text)value;
		        out.write(to.getBytes(), 0, to.getLength());
		        out.write("\n".getBytes("UTF-8"));
		}
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = job.getConfiguration();
		Path outDir = getOutputPath(job);
		FileSystem fs = outDir.getFileSystem(conf);
		String param[] = job.getJobName().split(":",-1);
		Path file = new Path(outDir,param[2]);
		FSDataOutputStream fileOut = fs.create(file,true);
	    return new SplitRecordWriter<K, V>(fileOut);
	}
	
	public void checkOutputSpecs(JobContext job) throws IOException{
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null) {
             throw new InvalidJobConfException("Output directory not set.");
        }
        Configuration conf = job.getConfiguration();
        String param[] = job.getJobName().split(":",-1);
        Path outfile = new Path(outDir,param[2]);
        FileSystem fs = outDir.getFileSystem(conf);
		if (fs.exists(outfile))
			throw new InvalidJobConfException("Output file alread exists.");
    }
}
