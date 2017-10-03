
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			/* The data should be like this before the processing */

			// line = this is cool\t20
			String line = value.toString().trim();

			// wordsPlusCount = [this is cool, 20]
			String[] wordsPlusCount = line.split("\t");

			if(wordsPlusCount.length < 2) { /* Edge case */
				return;
			}

			// words : [this, is, cool]
            // count: 20
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			
			if(count < threashold) { /* Also an edge */
				return;
			}

			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
				// this, is
			}

			/* Like a key value mapping relation */
			String outputKey = sb.toString().trim(); /* This is the key : this, is */
			String outputValue = words[words.length - 1]; /* This is the value : cool */
			
			if(!((outputKey == null) || (outputKey.length() < 1))) { /* make sure there is a output key */
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
                // this is --> cool = 20
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		/* get the n parameter from the configuration */
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// data -- -- -- -- this is, [girl = 50, boy = 60]

            // key : this is
            // value : [girl = 50, boy = 60]

            /* Temp cache */
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text val: values) {
				String curValue = val.toString().trim(); // girl = 50
				String word = curValue.split("=")[0].trim(); // girl
				int count = Integer.parseInt(curValue.split("=")[1].trim()); // 50
				if (tm.containsKey(count)) {
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}
			// <60, <boy...>> <50, <girl, bird>>
            // Use the iterator to write the data to the database
			Iterator<Integer> iter = tm.keySet().iterator();
			for (int j = 0; iter.hasNext() && j < n;) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for (String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
					j++;
				}
			}
		}
	}
}
