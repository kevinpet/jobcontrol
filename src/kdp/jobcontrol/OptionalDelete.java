package kdp.jobcontrol;

import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OptionalDelete extends ControlledFSAction {

  private Path path;
  private boolean recursive;

  public OptionalDelete(Configuration conf, Path path, boolean recursive) throws IOException {
    super(conf);
    init(path, recursive);
  }

  public OptionalDelete(FileSystem fs, Path path, boolean recursive) {
    super(fs);
    init(path, recursive);
  }
  
  public OptionalDelete(Configuration conf, String path, boolean recursive) throws IOException {
    this(conf, new Path(path), recursive);
  }

  public OptionalDelete(FileSystem fs, String path, boolean recursive) {
    this(fs, new Path(path), recursive);
  }

  private void init(Path path, boolean recursive) {
    this.path = path;
    this.recursive = recursive;
  }

  @Override
  protected void execute() throws IOException, InterruptedException {
    System.out.println("Deleting " + path + (recursive ? " recursively" : ""));
    try {
      fs.delete(path, recursive);
    } catch (FileNotFoundException e) {
      // ignore this is optional
    }
  }

}
