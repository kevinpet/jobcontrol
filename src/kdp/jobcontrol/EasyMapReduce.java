package kdp.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Literate wrapper for Job, ControlledJob, and Configuration
 */
public class EasyMapReduce extends ControlledJob {
  Configuration conf;
  public EasyMapReduce() throws IOException {
    conf = new Configuration();
    setJob(new Job(conf));
  }
  public EasyMapReduce with(String key, String value) {
    getJob().getConfiguration().set(key, value);
    return this;
  }
  public EasyMapReduce withJarForClass(Class<?> klass) {
    getJob().setJarByClass(klass);
    return this;
  }
  public EasyMapReduce withName(String jobName) {
    getJob().setJobName(jobName);
    return this;
  }
  public EasyMapReduce withMapOutputKeyValue(Class<? extends Writable> keyClass,
                                             Class<? extends Writable> valueClass) {
    getJob().setMapOutputKeyClass(keyClass);
    getJob().setMapOutputValueClass(valueClass);
    return this;
  }
  public EasyMapReduce withCombiner(Class<? extends Reducer> combinerClass) {
    getJob().setCombinerClass(combinerClass);
    return this;
  }
  public EasyMapReduce withMapper(Class<? extends Mapper> mapperClass) {
    getJob().setMapperClass(mapperClass);
    return this;
  }
  public EasyMapReduce withReducer(Class<? extends Reducer> reducerClass) {
    getJob().setReducerClass(reducerClass);
    return this;
  }
  public EasyMapReduce withNumReduceTasks(int numTasks) {
    getJob().setNumReduceTasks(numTasks);
    return this;
  }
  public EasyMapReduce withFileInput(String inputPath)
    throws IOException {
    return withFileInput(new Path(inputPath));
  }
  public EasyMapReduce withFileInput(Path inputPath) throws IOException {
    FileInputFormat.addInputPath(getJob(), inputPath);
    return this;
  }
  public EasyMapReduce withInputFormat(Class<? extends InputFormat> inputFormatClass)
    throws IOException {
    getJob().setInputFormatClass(inputFormatClass);
    return this;
  }
  public EasyMapReduce withOutputFormat(Class<? extends OutputFormat> outputFormatClass) {
    getJob().setOutputFormatClass(outputFormatClass);
    return this;
  }
  public EasyMapReduce withFileOutput(String outputPath) {
    return withFileOutput(new Path(outputPath));
  }
  public EasyMapReduce withFileOutput(Path outputPath) {
    FileOutputFormat.setOutputPath(getJob(), outputPath);
    return this;
  }
  public EasyMapReduce withOutputKeyValue(Class<? extends Writable> keyClass,
                                          Class<? extends Writable> valueClass) {
    getJob().setOutputKeyClass(keyClass);
    getJob().setOutputValueClass(valueClass);
    return this;
  }
  public EasyMapReduce withDependingJob(Controlled dependency) {
    addDependingJob(dependency);
    return this;
  }
  public EasyMapReduce withCounter(ControlledJob dependingJob,
                                   String groupName, String counterName,
                                   String propertyName) {
    if(requireCounter(dependingJob, groupName, counterName, propertyName)) {
        return this;
    } else {
      throw new IllegalStateException("Unable to add required counter, job has likely started");
    }
  }        
}