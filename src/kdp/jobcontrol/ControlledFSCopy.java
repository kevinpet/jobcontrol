package kdp.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ControlledFSCopy extends ControlledFSAction {

  Path from;
  Path to;

  public ControlledFSCopy(Configuration conf, Path from, Path to) throws IOException {
    super(conf);
    init(from, to);
  }

  public ControlledFSCopy(FileSystem fs, Path from, Path to) {
    super(fs);
    init(from, to);
  }
  
  private void init(Path from, Path to) {
    this.from = from;
    this.to = to;
  }

  @Override
  protected void execute() throws IOException, InterruptedException {
    fs.rename(from, to);
  }

}
