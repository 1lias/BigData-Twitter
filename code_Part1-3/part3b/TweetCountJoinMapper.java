package bdp.part3b;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetCountJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable athletesTable;

	private IntWritable one = new IntWritable(1);
	private Text textSport = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] values = value.toString().split("\t");

			if (values.length == 2) {

				try {
					if (values[0] != null && values[1] != null) {
						String name = values[0].trim();
						String sport = athletesTable.get(name).toString();
						int mentions = Integer.parseInt(values[1]);
						textSport.set(new Text(athletesTable.get(name).toString()));
						context.write(textSport, new IntWritable(mentions));
					}
				} catch (Exception e) {}
			}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athletesTable = new Hashtable<String,String>();

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
				if (fields.length > 7) {
					if ((fields[1]!=null) && (fields[7]!=null)){
						//fields are: 0:ID 1:Full Name 2:Country 3:Sex 4:Date of birth 5:Height 6:Weight 7:Sport 8:Gold Medals 9:Silver Medals 10:Bronze Medals
						athletesTable.put(fields[1],fields[7]);
					}
				}
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}
