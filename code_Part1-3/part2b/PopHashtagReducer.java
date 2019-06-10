package bdp.part2b;

import java.lang.InterruptedException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PopHashtagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    int sum = 0;

    for (IntWritable value : values) {
      sum = sum + value.get();
    }

    result.set(sum);
    context.write(key, result);
  }
}
