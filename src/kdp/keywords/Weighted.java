/**
 * 
 */
package kdp.keywords;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Weighted extends Mapper<LongWritable, Text, Text, Text> {
		static final String PROP_KEYWORDS_PATH = "kdp.keywords.weights";
		private Text words = new Text();
		private double defaultProb;
		private Map<String, Double> frequencies;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Counter<String> counter = new Counter<String>();
			String text = value.toString();
			for (String word : Common.tokens(text)) {
				if("".equals(word))
					continue;
				if(frequencies.containsKey(word)) {
					counter.increment(word, frequencies.get(word));
				} else {
					counter.increment(word, defaultProb);
				}
			}
			String mostFrequentWord = counter.maxKey();
			if(mostFrequentWord == null) {
				mostFrequentWord = "_NULL_";
			}
			words.set(mostFrequentWord);
			context.write(words, value);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			defaultProb = conf.getLong(Normalize.PROP_KEYWORDS_TOTAL, 0) / 2.0;
			String freqFiles = conf.get(PROP_KEYWORDS_PATH);
			frequencies = new HashMap<String, Double>();
			Path freqPath = new Path(freqFiles);
			FileSystem fs = FileSystem.get(conf);
			FileStatus fileStatus = fs.getFileStatus(freqPath);
			if (fileStatus.isDir()) {
				throw new IllegalArgumentException(
						"Can't handle a directory at this time");
			} else {
				loadFrequencies(freqPath, conf);
			}
		}

		private void loadFrequencies(Path path, Configuration conf) throws IOException {
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
			Text key = new Text();
			DoubleWritable value = new DoubleWritable();
			while(reader.next(key, value)) {
				frequencies.put(key.toString(), value.get());
			}
		}
	}