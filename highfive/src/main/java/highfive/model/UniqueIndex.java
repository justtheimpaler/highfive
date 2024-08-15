package highfive.model;

import java.util.List;

public class UniqueIndex {

  private Identifier name;
  private List<UniqueIndexMember> members;

  public UniqueIndex(Identifier name, List<UniqueIndexMember> columns) {
    this.name = name;
    this.members = columns;
  }

  public Identifier getName() {
    return name;
  }

  public List<UniqueIndexMember> getMembers() {
    return members;
  }

  public static class UniqueIndexMember {

    private Identifier column;
    private boolean ascending;

    public UniqueIndexMember(Identifier column, boolean ascending) {
      this.column = column;
      this.ascending = ascending;
    }

    public Identifier getColumn() {
      return column;
    }

    public boolean isAscending() {
      return ascending;
    }

  }

}
