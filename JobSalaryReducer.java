package task1;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobSalaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable(); 

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    	// initialize a variable to store the sum of salaries for a given job title
        long sum = 0;
     // initialize a variable to keep track of the number of salaries for a given job title
        long count = 0; 
     // iterate over the list of salaries for a given job title
        for (LongWritable val : values) { 
        	// add each salary to the sum
            sum += val.get(); 
         // increment the count of salaries
            count++; 
        }
     // calculate the average salary
        long average = sum / count; 
     // set the result to the average salary
        result.set(average); 
     // write the job title and average salary as output
        context.write(key, result); 
    }
}
