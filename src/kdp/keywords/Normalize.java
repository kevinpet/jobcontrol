/**
 * 
 */
package kdp.keywords;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Normalize extends
		Mapper<Text, IntWritable, Text, DoubleWritable> {
	static final String PROP_KEYWORDS_TOTAL = "kdp.keywords.total";
	private double count;
	private DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		if ("".equals(key.toString()))
			return;
		int occurrences = Integer.parseInt(value.toString());
		if (occurrences > 2) {
			outValue.set(((double) count) / ((double) occurrences));
			context.write(key, outValue);
			context.getCounter("words", "repeated").increment(1l);
		} else {
			context.getCounter("words", "singletons").increment(1l);
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		count = context.getConfiguration().getInt(PROP_KEYWORDS_TOTAL, -1);
		if (count < 1)
			throw new IllegalArgumentException(
					"Must provide positive count of words as \""
							+ PROP_KEYWORDS_TOTAL + "\"");
	}

	static void setTotalWords(Configuration jobConf, int totalWords) {
		jobConf.setInt(PROP_KEYWORDS_TOTAL, totalWords);
	}
}