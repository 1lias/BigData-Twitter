package bdp.part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.InterruptedException;
import java.io.IOException;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TweetTimeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  private final IntWritable one = new IntWritable(1);

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    String[] line = value.toString().split(";");
    /*
      Each line is devided into four parts:
            0       1               2                  3
      epoch_time;tweetId;tweet(including #hashtags);device
    */
    if (line.length == 4) {
      try {
        long epoch_time = Long.parseLong(line[0]);
        LocalDateTime time = LocalDateTime.ofEpochSecond(epoch_time/1000,0,ZoneOffset.of("-2"));


        IntWritable timeGroup = new IntWritable(time.getHour());
        context.write(timeGroup,one);
      } catch (Exception e) {}
    }
  }
}
