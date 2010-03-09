package kdp.keywords;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
	public static class Count extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text out = new Text();
		private IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String text = value.toString();
			for (String word : Common.tokens(text)) {
				if("".equals(word))
					continue;
				out.set(word);
				context.getCounter("words", "all").increment(1l);
				context.write(out, one);
			}
		}
	}

	public static class Collect extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			IntWritable value = new IntWritable(sum);
			context.write(key, value);
		}
	}

}
		