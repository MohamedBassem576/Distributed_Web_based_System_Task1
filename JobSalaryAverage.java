
package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Define the JobSalaryAverage class with a main method that throws an exception
public class JobSalaryAverage {
    public static void main(String[] args) throws Exception {
        // Create a new Configuration object to hold the job's configuration properties
        Configuration conf = new Configuration();
        
        // Create a new Job object with the given configuration and a name for the job
        Job job = Job.getInstance(conf, "job salary average");
        
        // Specify the main class of the job
        job.setJarByClass(JobSalaryAverage.class);
        
        // Specify the mapper class for the job
        job.setMapperClass(JobSalaryMapper.class);
        
        // Specify the reducer class for the job
        job.setReducerClass(JobSalaryReducer.class);
        
        // Specify the output key class for the job
        job.setOutputKeyClass(Text.class);
        
        // Specify the output value class for the job
        job.setOutputValueClass(LongWritable.class);
        
        // Specify the input path for the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Specify the output path for the job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Wait for the job to complete and exit with a status of 0 if it completed successfully or 1 if it failed
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

