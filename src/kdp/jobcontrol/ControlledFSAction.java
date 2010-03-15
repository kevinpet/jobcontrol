package kdp.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public abstract class ControlledFSAction extends Controlled {
  protected FileSystem fs;

  
  public ControlledFSAction(FileSystem fs) {
    super();
    this.fs = fs;
  }

  public ControlledFSAction(Configuration conf) throws IOException {
    this.fs = FileSystem.get(conf);
  }

  @Override
  protected void checkRunningState() throws IOException, InterruptedException {
    try {
      System.out.println("Executing " + getName() + ", state is now " + state);
      execute();
      state = State.SUCCESS;
      System.out.println("Executed " + getName() + ", state is now " + state);
    } catch (IOException e) {
      System.out.println(getName() + " failed");
      e.printStackTrace();
      state = State.FAILED;
      System.out.println("Failed " + getName() + ", state is now " + state);
    }
  }

  @Override
  public void killJob() throws IOException, InterruptedException {
    // no-op, we've either succeeded, or failed, but running isn't an extended process
  }

  @Override
  protected void submit() {
    state = State.RUNNING;
    System.out.println("Submitting " + getName() + ", state is now " + state);
  }

  protected abstract void execute() throws IOException, InterruptedException;

}
