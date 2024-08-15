package highfive.model;

import java.util.Collections;
import java.util.List;

public class Table {

  private Identifier identifier;
  private List<Column> columns;

  public Table(Identifier identifier, List<Column> columns) {
    this.identifier = identifier;
    this.columns = columns;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public List<Column> getColumns() {
    return columns;
  }

  // Utilities

  void sortColumns() {
    Collections.sort(this.columns);
  }

}
