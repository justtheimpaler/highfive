package highfive.model;

import java.util.Set;
import java.util.stream.Collectors;

public class ColumnFilter implements OptionalProperty {

  private Set<String> allowedColumns;

  public ColumnFilter(Set<String> allowedColumns) {
    this.allowedColumns = allowedColumns;
    if (this.allowedColumns == null || this.allowedColumns.isEmpty()) {
      this.allowedColumns = null;
    } else {
      this.allowedColumns = this.allowedColumns.stream().map(n -> n.toLowerCase()).collect(Collectors.toSet());
    }
  }

  public boolean accepts(String canonicalName) {
    if (this.allowedColumns == null) {
      return true;
    }
    if (canonicalName == null) {
      return false;
    }
    return this.allowedColumns.contains(canonicalName.toLowerCase());
  }

  public String render() {
    return this.allowedColumns == null ? "" : this.allowedColumns.stream().collect(Collectors.joining(","));
  }

  @Override
  public boolean declared() {
    return this.allowedColumns != null;
  }

}
