package kdp;

public class Container<T> {
  private T contents;
  public T get() {
    return contents;
  }
  public void set(T contents) {
    this.contents = contents;
  }
}
