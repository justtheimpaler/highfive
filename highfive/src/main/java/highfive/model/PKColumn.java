package highfive.model;

public class PKColumn {

  private int position;
  private String canonicalName;

  public PKColumn(int position, String canonicalName) {
    this.position = position;
    this.canonicalName = canonicalName;
  }

  public int getPosition() {
    return position;
  }

  public String getCanonicalName() {
    return canonicalName;
  }

}
