package kdp.jobcontrol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class Controlled {

  public static enum State {
    SUCCESS, WAITING, RUNNING, READY, FAILED, DEPENDENT_FAILED
  }

  protected State state;
  protected String controlID;
  protected String message;
  protected List<Controlled> dependingJobs;

  public Controlled() {
    super();
    this.state = State.WAITING;
    this.controlID = "unassigned";
    this.message = "just initialized";
  }

  public String getName() {
    return this.getClass().getName() + getJobID();
  }
  
  /**
   * @return the job ID of this job assigned by JobControl
   */
  public String getJobID() {
    return this.controlID;
  }

  /**
   * Set the job ID for this job.
   * 
   * @param id
   *          the job ID
   */
  public void setJobID(String id) {
    this.controlID = id;
  }

  /**
   * @return the state of this job
   */
  public synchronized State getJobState() {
    return this.state;
  }

  /**
   * Set the state for this job.
   * 
   * @param state
   *          the new state for this job.
   */
  protected synchronized void setJobState(State state) {
    this.state = state;
  }

  /**
   * @return the message of this job
   */
  public synchronized String getMessage() {
    return this.message;
  }

  /**
   * Set the message for this job.
   * 
   * @param message
   *          the message for this job.
   */
  public synchronized void setMessage(String message) {
    this.message = message;
  }

  /**
   * @return the depending jobs of this job
   */
  public List<Controlled> getDependentJobs() {
    return this.dependingJobs;
  }

  /**
   * Add a job to this jobs' dependency list. Dependent jobs can only be added
   * while a Job is waiting to run, not during or afterwards.
   * 
   * @param dependingJob
   *          Job that this Job depends on.
   * @return <tt>true</tt> if the Job was added.
   */
  public synchronized boolean addDependingJob(Controlled dependingJob) {
    if (this.state == State.WAITING) { // only allowed to add jobs when waiting
      if (this.dependingJobs == null) {
        this.dependingJobs = new ArrayList<Controlled>();
      }
      return this.dependingJobs.add(dependingJob);
    } else {
      return false;
    }
  }

  /**
   * @return true if this job is in a complete state
   */
  public synchronized boolean isCompleted() {
    return this.state == State.FAILED || this.state == State.DEPENDENT_FAILED
        || this.state == State.SUCCESS;
  }

  /**
   * @return true if this job is in READY state
   */
  public synchronized boolean isReady() {
    return this.state == State.READY;
  }

  /**
   * Check and update the state of this job. The state changes depending on its
   * current state and the states of the depending jobs.
   */
  protected synchronized State checkState() throws IOException,
      InterruptedException {
        if (this.state == State.RUNNING) {
          checkRunningState();
        }
        if (this.state != State.WAITING) {
          return this.state;
        }
        if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
          this.state = State.READY;
          return this.state;
        }
        Controlled pred = null;
        int n = this.dependingJobs.size();
        for (int i = 0; i < n; i++) {
          pred = this.dependingJobs.get(i);
          State s = pred.checkState();
          if (s == State.WAITING || s == State.READY || s == State.RUNNING) {
            break; // a pred is still not completed, continue in WAITING
            // state
          }
          if (s == State.FAILED || s == State.DEPENDENT_FAILED) {
            this.state = State.DEPENDENT_FAILED;
            this.message = "depending job " + i + " with jobID " + pred.getJobID()
                + " failed. " + pred.getMessage();
            break;
          }
          // pred must be in success state
          if (i == n - 1) {
            this.state = State.READY;
          }
        }
      
        return this.state;
      }

  protected abstract void checkRunningState() throws IOException, InterruptedException;

  protected abstract void killJob() throws IOException, InterruptedException;

  protected abstract void submit();

}