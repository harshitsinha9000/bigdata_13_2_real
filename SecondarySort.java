

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SecondarySort {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	    Job job = Job.getInstance();

	    Path input = new Path(args[0]);
	    Path output = new Path(args[1]);

	    Configuration conf = new Configuration();
	    
	    FileSystem fs = output.getFileSystem(conf);
	    fs.delete(output, true);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    	    
	    job.setJobName("Secondary Sort Program");
	    job.setJarByClass(SecondarySort.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapOutputKeyClass(IntPair.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(IntPair.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(SecondarySortMapper.class);
	    	    
	    job.setReducerClass(SecondarySortReducer.class);
	    job.setPartitionerClass(FirstPartitioner.class);
	    
	    job.setSortComparatorClass(SortComparator.class);
	    job.setGroupingComparatorClass(GroupingComparator.class);
	    
	    job.waitForCompletion(true);

	}
}