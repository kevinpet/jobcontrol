package kdp.keywords;

import java.io.IOException;
import java.util.Calendar;

import kdp.jobcontrol.*;
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
public class EasyKeywords {

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

    EasyMapReduce wordcount = new EasyMapReduce()
      .withName("word count")
      .withJarForClass(EasyKeywords.class)
      .withMapOutputKeyValue(Text.class, IntWritable.class)
      .withCombiner(Collect.class)
      .withMapper(Count.class)
      .withReducer(Collect.class)
      .withFileInput(input)
      .withInputFormat(TextInputFormat.class)
      .withFileOutput(counts)
      .withOutputFormat(SequenceFileOutputFormat.class)
      .withOutputKeyValue(Text.class, IntWritable.class);
    control.addJob(wordcount);
    System.out.println("Counting words to " + counts);

    // normalize values
    EasyMapReduce normalize = new EasyMapReduce()
      .withName("normalize counts")
      .withJarForClass(EasyKeywords.class)
      .withMapper(Normalize.class)
      .withNumReduceTasks(1)
      .withFileInput(counts)
      .withFileOutput(normalized)
      .withMapOutputKeyValue(Text.class, DoubleWritable.class)
      .withInputFormat(SequenceFileInputFormat.class)
      .withOutputFormat(SequenceFileOutputFormat.class)
      .withOutputKeyValue(Text.class, DoubleWritable.class)
      .withDependingJob(wordcount)
      .withCounter(wordcount, "words", "all", Normalize.PROP_KEYWORDS_TOTAL);
    System.out.println("Normalizing counts to " + normalized);
    control.addJob(normalize);

    // scale counts and filter
    EasyMapReduce extract = new EasyMapReduce()
      .with(Weighted.PROP_KEYWORDS_PATH, normalized + "/part-r-00000")
      .withName("extract weighted keywords")
      .withJarForClass(KeywordJobControl.class)
      .withMapper(Weighted.class)
      .withNumReduceTasks(1)
      .withFileInput(input)
      .withFileOutput(weighted)
      .withOutputKeyValue(Text.class, Text.class)
      .withDependingJob(normalize);
    System.out.println("Extracting top keyword to " + weighted);
    control.addJob(extract);

    // move old output to a temp location
    ControlledFSAction moveOldAside = new OptionalRename(conf, output, output + "_old");
    moveOldAside.addDependingJob(extract);
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
