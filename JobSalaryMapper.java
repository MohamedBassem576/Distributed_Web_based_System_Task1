package task1;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobSalaryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // Declare a private instance variable jobTitle of type Text
    private Text jobTitle = new Text();

    // Declare a private instance variable salary of type LongWritable
    private LongWritable salary = new LongWritable();

    // Override the map method of the Mapper class to process each input record
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Split the input record into an array of strings using the comma as a delimiter
        String[] line = value.toString().split(",");

        // Set the jobTitle variable to the second element of the array
        jobTitle.set(line[1]);

        // Set the salary variable to the third element of the array, parsed as a Long value
        salary.set(Long.parseLong(line[2]));

        // Write the jobTitle and salary variables to the output context
        context.write(jobTitle, salary);
    }
}
