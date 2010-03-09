package kdp.keywords;

import java.io.IOException;
import java.util.Calendar;

import kdp.keywords.WordCount.Collect;
import kdp.keywords.WordCount.Count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Sequence of Map Reduce jobs to calculate the most important keyword of a piece of text.
 */
public class Keywords {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] myArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Calendar date = Calendar.getInstance();
		String batch = String.format("batch_%02d%02d", date
				.get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE));
		String input = myArgs[0];
		String counts = batch + "/counts";
		String normalized = batch + "/norm";
		String weighted = batch + "/weighted";

		Job wordcount = new Job(conf, "word count");
		wordcount.setJarByClass(Keywords.class);
		wordcount.setMapOutputKeyClass(Text.class);
		wordcount.setMapOutputValueClass(IntWritable.class);
		wordcount.setCombinerClass(Collect.class);
		wordcount.setMapperClass(Count.class);
		wordcount.setReducerClass(Collect.class);
		FileInputFormat.setInputPaths(wordcount, new Path(input));
		wordcount.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(wordcount, new Path(counts));
		wordcount.setOutputFormatClass(SequenceFileOutputFormat.class);
		wordcount.setOutputKeyClass(Text.class);
		wordcount.setOutputValueClass(IntWritable.class);
		System.out.println("Counting words to " + counts);
		if (!wordcount.waitForCompletion(true))
			System.exit(1);

		// get total words out of previous job
		long totalWords = wordcount.getCounters().getGroup("words")
				.findCounter("all").getValue();
		System.out.println("Total of " + totalWords + " words");

		// normalize values
		conf.set(Normalize.PROP_KEYWORDS_TOTAL, Long.toString(totalWords));
		Job normalize = new Job(conf, "normalize counts");
		normalize.setJarByClass(Keywords.class);
		normalize.setMapperClass(Normalize.class);
		normalize.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(normalize, new Path(counts));
		FileOutputFormat.setOutputPath(normalize, new Path(normalized));
		normalize.setMapOutputKeyClass(Text.class);
		normalize.setMapOutputValueClass(DoubleWritable.class);
		normalize.setInputFormatClass(SequenceFileInputFormat.class);
		normalize.setOutputFormatClass(SequenceFileOutputFormat.class);
		normalize.setOutputKeyClass(Text.class);
		normalize.setOutputValueClass(DoubleWritable.class);
		System.out.println("Normalizing counts to " + normalized);
		if (!normalize.waitForCompletion(true)) {
			System.err.println("Normalize failed");
			System.exit(1);
		}

		// scale counts and filter
		conf.set(Weighted.PROP_KEYWORDS_PATH, normalized + "/part-r-00000");
		// conf.set("kdp.keywords.count", myArgs[1]);
		Job extract = new Job(conf, "extract weighted keywords");
		extract.setJarByClass(Keywords.class);
		extract.setMapperClass(Weighted.class);
		extract.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(extract, new Path(input));
		FileOutputFormat.setOutputPath(extract, new Path(weighted));
		extract.setInputFormatClass(TextInputFormat.class);
		extract.setOutputFormatClass(TextOutputFormat.class);
		extract.setOutputKeyClass(Text.class);
		extract.setOutputValueClass(Text.class);
		System.out.println("Extracting top keyword to " + weighted);
		if (!extract.waitForCompletion(true))
			System.exit(1);
	}

}
