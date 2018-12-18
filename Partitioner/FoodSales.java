package foodmart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FoodSales extends Configured implements Tool{

	private static final String homeDir = "/user/cloudera/";
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName(this.getClass().getName());
		job.setJarByClass(getClass());
		String dir = homeDir + this.getClass().getPackage().toString().substring(8)+"/";
		//input path (where is your table)
		TextInputFormat.addInputPath(job, new Path(dir+args[0]));
		//output
		TextOutputFormat.setOutputPath(job, new Path(dir+args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SalesMapper.class);
		//combiner is not needed
		// job.setCombinerClass(SalesReducer.class);
		job.setReducerClass(SalesReducer.class);
		job.setNumReduceTasks(2);
		
		
		job.setPartitionerClass(SalesPartitioner.class);
		job.setGroupingComparatorClass(SalesComparator.class);
		//Add distributed Cache
		/*
		try {
			job.addCacheFile(new URI(dir+args[2]+"#Promos"));
		}catch(Exception e){
			System.out.println(e);
		}
		URI[] cacheFiles= job.getCacheFiles();
		 if(cacheFiles != null) {
			 for (URI cacheFile : cacheFiles) {
				 System.out.println("Cache file ->" + cacheFile);
			 }
		 }
		 */ 	
		// configure output
		//If map and reduce have different formats
		job.setMapOutputKeyClass(SalesCKey.class);
		job.setMapOutputValueClass(Text.class);
		//output for everything
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FoodSales(), args);
		System.exit(exitCode);
	}
}