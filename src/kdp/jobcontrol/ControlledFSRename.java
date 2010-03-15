package kdp.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ControlledFSRename extends ControlledFSAction {

  Path from;
  Path to;

  public ControlledFSRename(Configuration conf, Path from, Path to) throws IOException {
    super(conf);
    init(from, to);
  }

  public ControlledFSRename(FileSystem fs, Path from, Path to) {
    super(fs);
    init(from, to);
  }

  public ControlledFSRename(Configuration conf, String from, String to) throws IOException {
    super(conf);
    init(from, to);
  }

  public ControlledFSRename(FileSystem fs, String from, String to) {
    super(fs);
    init(from, to);
  }

  private void init(String from, String to) {
    init(new Path(from), new Path(to));
  }

  private void init(Path from, Path to) {
    this.from = from;
    this.to = to;
  }

  @Override
  protected void execute() throws IOException, InterruptedException {
    System.out.println("Renaming " + from + " to " + to);
    fs.rename(from, to);
  }

}
