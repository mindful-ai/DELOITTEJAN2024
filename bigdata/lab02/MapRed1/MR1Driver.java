import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class MR1Driver extends Configured implements Tool{
     
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MR1Driver(), args);
        System.exit(exitCode);
    }
  
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }
     
        //Create a new Jar and set the driver class(this class) as the main class of jar
        Job job = new Job();
        job.setJarByClass(MR1Driver.class);
        job.setJobName("WordCounter");
         
        //Set the input and the output path from the arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        //Set the map and reduce classes in the job
        job.setMapperClass(MR1Mapper.class);
        job.setReducerClass(MR1Reducer.class);
     
        //Run the job and wait for its completion
        int returnValue = job.waitForCompletion(true) ? 0:1;
         
        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");           
        }
         
        return returnValue;
    }
}
