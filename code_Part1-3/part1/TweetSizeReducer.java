package bdp.part1;

import java.lang.InterruptedException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetSizeReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

  private IntWritable result = new IntWritable();

  public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    int sum = 0;

    for (IntWritable value : values) {
      sum = sum + value.get();
    }

    result.set(sum);
    Text finalKey =new Text((key.get()*5-4)+"-"+(key.get()*5));
    context.write(finalKey, result);
  }
}
