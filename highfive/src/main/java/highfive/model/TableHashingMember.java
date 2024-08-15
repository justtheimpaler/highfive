package highfive.model;

public class TableHashingMember {

  private String generricColumnName;
  private boolean ascending;
  private Boolean nullsFirst;

  public TableHashingMember(String genericColumnName, boolean ascending, Boolean nullsFirst) {
    this.generricColumnName = genericColumnName;
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
  }

  public String getGenericColumnName() {
    return generricColumnName;
  }

  public boolean isAscending() {
    return ascending;
  }

  public Boolean getNullsFirst() {
    return nullsFirst;
  }

}
