package kdp.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ControlledFSDelete extends ControlledFSAction {

  private Path path;
  private boolean recursive;

  public ControlledFSDelete(Configuration conf, Path path, boolean recursive) throws IOException {
    super(conf);
    init(path, recursive);
  }

  public ControlledFSDelete(FileSystem fs, Path path, boolean recursive) {
    super(fs);
    init(path, recursive);
  }
  
  public ControlledFSDelete(Configuration conf, String path, boolean recursive) throws IOException {
    this(conf, new Path(path), recursive);
  }

  public ControlledFSDelete(FileSystem fs, String path, boolean recursive) {
    this(fs, new Path(path), recursive);
  }

  private void init(Path path, boolean recursive) {
    this.path = path;
    this.recursive = recursive;
  }

  @Override
  protected void execute() throws IOException, InterruptedException {
    System.out.println("Deleting " + path + (recursive ? " recursively" : ""));
    fs.delete(path, recursive);
  }

}
