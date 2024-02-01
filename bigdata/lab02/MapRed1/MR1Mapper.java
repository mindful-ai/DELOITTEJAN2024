import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * In the mapper we get the input record key as byte offset which is of LongWritable type and the value as the entire record.
 * As we know the record is a line of text with each field separated by a tab character.
 * We split the record based on the tab character into an array of strings.
 * We then take the 2nd element of this array and use it as mapper's output key.
 * 1 is set (hard-coded) as mapper's output value. Both these are written to context i.e the output of mapper.
 */

public class MR1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {

		String s = value.toString();
	    String[] fields = s.split("\t");

	    if (fields[2].length() > 0) {
	    	context.write(new Text(fields[2]), new IntWritable(1));
	    }
		
	  }
}
