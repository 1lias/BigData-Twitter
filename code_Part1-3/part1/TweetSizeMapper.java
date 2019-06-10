package bdp.part1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.InterruptedException;
import java.io.IOException;

public class TweetSizeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  private final IntWritable one = new IntWritable(1);

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    String[] line = value.toString().split(";");
    /*
      Each line is devided into four parts:
            0       1               2                  3
      epoch_time;tweetId;tweet(including #hashtags);device
    */
    if (line.length == 4) {

      int length = line[2].length();
      if (length <= 140) {

        context.write(group(length), one);
      }
    }
  }

  // Takes an input an integer and makes it work with the Reducer to put each key into a group
  public IntWritable group(int value) {

    float valueFloat = (float) value/5;
    int group = (int) Math.ceil(valueFloat);

    return new IntWritable(group);
  }
}
