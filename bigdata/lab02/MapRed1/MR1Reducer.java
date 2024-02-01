import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * In reducer we get record's key & value as the company name and the collection of 1s.
 * The number of in the value 1s will be as many as they occur in the input file.
 * We then initiate a variable counter to 0 and in a loop which iterates as many times as the collection size,
 * we add value (hard-coded 1) to this counter. So by the end of the loop we will have the number of occurrences in the counter.
 * In the reducer's output key & value, we write the key as is and the counter as the value.
 * 
 */

public class MR1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {

		int cmpnyCount = 0;
	    for (IntWritable value : values) {
	      cmpnyCount += value.get();
	    }
	    context.write(key, new IntWritable(cmpnyCount));
	  }
	
}
