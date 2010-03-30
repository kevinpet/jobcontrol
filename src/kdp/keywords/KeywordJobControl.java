package kdp.keywords;

import java.io.IOException;
import java.util.Calendar;

import kdp.jobcontrol.ControlledFSAction;
import kdp.jobcontrol.ControlledFSDelete;
import kdp.jobcontrol.ControlledFSRename;
import kdp.jobcontrol.ControlledJob;
import kdp.jobcontrol.JobControl;
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
 * Sequence of Map Reduce jobs to calculate the most important keyword of a
 * piece of text. Uses JobControl to manage jobs.
 */
public class KeywordJobControl {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    JobControl control = new JobControl("keywords flow");
    Configuration conf = new Configuration();
    String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Calendar date = Calendar.getInstance();
    String batch = String.format("batch_%02d%02d", date
        .get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE));
    String input = myArgs[0];
    String output = myArgs[1];
    String counts = batch + "/counts";
    String normalized = batch + "/norm";
    String weighted = batch + "/weighted";

    Job wordcount = new Job(conf, "word count");
    wordcount.setJarByClass(KeywordJobControl.class);
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
    ControlledJob wordcountControl = new ControlledJob(wordcount, null);
    control.addJob(wordcountControl);
    System.out.println("Counting words to " + counts);

    // normalize values
    Job normalize = new Job(conf, "normalize counts");
    normalize.setJarByClass(KeywordJobControl.class);
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
    ControlledJob normControl = new ControlledJob(normalize, null);
    normControl.addDependingJob(wordcountControl);
    // Not supported in upstream version, I pass the total value to the next job in the chain
    normControl.requireCounter(wordcountControl, "words", "all",
        Normalize.PROP_KEYWORDS_TOTAL);
    System.out.println("Normalizing counts to " + normalized);
    control.addJob(normControl);

    // scale counts and filter
    conf.set(Weighted.PROP_KEYWORDS_PATH, normalized + "/part-r-00000");
    Job extract = new Job(conf, "extract weighted keywords");
    extract.setJarByClass(KeywordJobControl.class);
    extract.setMapperClass(Weighted.class);
    extract.setNumReduceTasks(1);
    FileInputFormat.setInputPaths(extract, new Path(input));
    FileOutputFormat.setOutputPath(extract, new Path(weighted));
    extract.setInputFormatClass(TextInputFormat.class);
    extract.setOutputFormatClass(TextOutputFormat.class);
    extract.setOutputKeyClass(Text.class);
    extract.setOutputValueClass(Text.class);
    ControlledJob extractControl = new ControlledJob(extract, null);
    extractControl.addDependingJob(normControl);
    System.out.println("Extracting top keyword to " + weighted);
    control.addJob(extractControl);

    // move old output to a temp location
    ControlledFSAction moveOldAside = new OptionalRename(conf, output, output + "_old");
    moveOldAside.addDependingJob(extractControl);
    control.addJob(moveOldAside);

    // move new stuff to final location
    ControlledFSAction moveOutput = new ControlledFSRename(conf, weighted, output);
    moveOutput.addDependingJob(moveOldAside);
    control.addJob(moveOutput);

    // delete batch
    ControlledFSAction deleteTemp = new ControlledFSDelete(conf, batch, true);
    deleteTemp.addDependingJob(moveOutput);
    control.addJob(deleteTemp);

    // delete old output
    ControlledFSAction deleteOld = new OptionalDelete(conf, output + "_old", true);
    deleteOld.addDependingJob(moveOutput);
    control.addJob(deleteOld);

    // added thread handling to parent class for this simple use
    control.waitForCompletion(50);
  }

}
