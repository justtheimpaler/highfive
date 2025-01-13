package highfive.model;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

  public Column findColumn(String genericColumnName) {
    for (Column col : this.columns) {
      if (col.getCanonicalName().equalsIgnoreCase(genericColumnName)) {
        return col;
      }
    }
    return null;
  }

  public List<Column> getPKColumns() {
    return this.columns.stream().filter(c -> c.getPKPosition() != null).collect(Collectors.toList());
  }

}
