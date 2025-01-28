package highfive.model;

public class TableHash {

  private String hash;
  private boolean nonDeterministic;
  private boolean failed;
  private long rowCount;

  public TableHash(String hash, boolean nonDeterministic, boolean failed, long rowCount) {
    super();
    this.hash = hash;
    this.nonDeterministic = nonDeterministic;
    this.failed = failed;
    this.rowCount = rowCount;
  }

  public String getHash() {
    return hash;
  }

  public boolean isNonDeterministic() {
    return nonDeterministic;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getRowCount() {
    return rowCount;
  }

}
