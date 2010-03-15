/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kdp.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * This class encapsulates a MapReduce job and its dependency. It monitors the
 * states of the depending jobs and updates the state of this job. A job starts
 * in the WAITING state. If it does not have any depending jobs, or all of the
 * depending jobs are in SUCCESS state, then the job state will become READY. If
 * any depending jobs fail, the job will fail too. When in READY state, the job
 * can be submitted to Hadoop for execution, with the state changing into
 * RUNNING state. From RUNNING state, the job can get into SUCCESS or FAILED
 * state, depending the status of the job execution.
 */

public class ControlledJob extends Controlled {

  protected class RequiredCounter {
    private ControlledJob dependingJob;
    private String groupName;
    private String counterName;
    private String propertyName;
    protected RequiredCounter(ControlledJob dependingJob, String groupName,
        String counterName, String propertyName) {
      this.dependingJob = dependingJob;
      this.groupName = groupName;
      this.counterName = counterName;
      this.propertyName = propertyName;
    }
    protected long getCounter() throws IOException {
      return dependingJob.getJob().getCounters().findCounter(groupName, counterName).getValue();
    }
    protected String getPropertyName() {
      return propertyName;
    }
  }

  public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";
  private Job job; // mapreduce job to be executed.
  private List<RequiredCounter> requiredCounters;

  /**
   * Construct a job.
   * 
   * @param job
   *          a mapreduce job to be executed.
   * @param dependingJobs
   *          an array of jobs the current job depends on
   */
  public ControlledJob(Job job, List<Controlled> dependingJobs)
      throws IOException {
    super();
    this.job = job;
    this.dependingJobs = dependingJobs;
  }

  /**
   * Construct a job.
   * 
   * @param conf
   *          mapred job configuration representing a job to be executed.
   * @throws IOException
   */
  public ControlledJob(Configuration conf) throws IOException {
    this(new Job(conf), null);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("job name:\t").append(this.job.getJobName()).append("\n");
    sb.append("job id:\t").append(this.controlID).append("\n");
    sb.append("job state:\t").append(this.state).append("\n");
    sb.append("job mapred id:\t").append(this.job.getJobID()).append("\n");
    sb.append("job message:\t").append(this.message).append("\n");

    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      sb.append("job has no depending job:\t").append("\n");
    } else {
      sb.append("job has ").append(this.dependingJobs.size()).append(
          " dependeng jobs:\n");
      for (int i = 0; i < this.dependingJobs.size(); i++) {
        sb.append("\t depending job ").append(i).append(":\t");
        sb.append((this.dependingJobs.get(i)).getName()).append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * @return the job name of this job
   */
  public String getName() {
    return job.getJobName();
  }

  /**
   * @return the mapred ID of this job as assigned by the mapred framework.
   */
  public JobID getMapredJobID() {
    return this.job.getJobID();
  }

  /**
   * @return the mapreduce job
   */
  public synchronized Job getJob() {
    return this.job;
  }

  /**
   * Set the mapreduce job
   * 
   * @param job
   *          the mapreduce job for this job.
   */
  public synchronized void setJob(Job job) {
    this.job = job;
  }

  /**
   * Make the value of a counter in a depending job available to this job. Note
   * that this does not add the dependency -- that must be done separately.
   * 
   * @param dependingJob
   *          the job that generates the counter
   * @param groupName
   * @param counterName
   * @param propertyName
   * @return
   */
  public synchronized boolean requireCounter(ControlledJob dependingJob,
      String groupName, String counterName, String propertyName) {
    if (this.state == State.WAITING) {
      if (this.requiredCounters == null) {
        this.requiredCounters = new ArrayList<RequiredCounter>();
      }
      return this.requiredCounters.add(new RequiredCounter(dependingJob, groupName, counterName, propertyName));
    } else {
      return false;
    }
  }

  @Override
  public void killJob() throws IOException, InterruptedException {
    job.killJob();
  }

  /**
   * Check the state of this running job. The state may remain the same, become
   * SUCCESS or FAILED.
   */
  @Override
  protected void checkRunningState() throws IOException, InterruptedException {
    try {
      if (job.isComplete()) {
        if (job.isSuccessful()) {
          this.state = State.SUCCESS;
        } else {
          this.state = State.FAILED;
          this.message = "Job failed!";
        }
      }
    } catch (IOException ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (job != null) {
          job.killJob();
        }
      } catch (IOException e) {
      }
    }
  }

  /**
   * Submit this job to mapred. The state becomes RUNNING if submission is
   * successful, FAILED otherwise.
   */
  protected synchronized void submit() {
    try {
      Configuration conf = job.getConfiguration();
      if (conf.getBoolean(CREATE_DIR, false)) {
        FileSystem fs = FileSystem.get(conf);
        Path inputPaths[] = FileInputFormat.getInputPaths(job);
        for (int i = 0; i < inputPaths.length; i++) {
          if (!fs.exists(inputPaths[i])) {
            try {
              fs.mkdirs(inputPaths[i]);
            } catch (IOException e) {

            }
          }
        }
      }
      if (requiredCounters != null) {
        for(RequiredCounter counter : requiredCounters) {
          conf.set(counter.getPropertyName(), Long.toString(counter.getCounter()));
        }
      }
      job.submit();
      this.state = State.RUNNING;
    } catch (Exception ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
    }
  }

}
