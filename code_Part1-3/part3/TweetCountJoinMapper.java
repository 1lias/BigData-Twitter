package bdp.part3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetCountJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private HashSet athleteSet;

	private IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] tweetValues = value.toString().split(";"); //Split value into tweet values
      //Each line is devided into four parts:
      //      0       1               2                  3
      //epoch_time;tweetId;tweet(including #hashtags);device
			if (tweetValues.length > 2) {
				String tweet = tweetValues[2];

				Iterator itr2 = athleteSet.iterator();
				while (itr2.hasNext()) {
					String name = itr2.next().toString();
					if(tweet.contains(name)) {
						context.write(new Text(name), one);
					}
				}
			}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athleteSet = new HashSet<String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {

				String[] fields = line.split(",");
				if (fields.length == 11) {
					//fields are: 0:ID 1:Full Name 2:Country 3:Sex 4:Date of birth 5:Height 6:Weight 7:Sport 8:Gold Medals 9:Silver Medals 10:Bronze Medals
					athleteSet.add(" " + fields[1]);
				}
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}
