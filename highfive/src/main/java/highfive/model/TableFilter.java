package highfive.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import highfive.utils.Name;

public class TableFilter implements OptionalProperty {

  private List<Name> allowedTables;
  private List<Name> acceptedTables;

  public TableFilter(Set<String> allowedTables) {
    if (allowedTables == null || allowedTables.isEmpty()) {
      this.allowedTables = null;
    } else {
      this.allowedTables = allowedTables.stream().map(n -> Name.of(n)).collect(Collectors.toList());
    }
    this.reset();
  }

  public void reset() {
    this.acceptedTables = new ArrayList<>();
  }

  public boolean allTablesFound() {
    return this.allowedTables == null || this.acceptedTables.size() >= this.allowedTables.size();
  }

  public int found() {
    return this.acceptedTables.size();
  }

  public boolean accepts(String canonicalName) {
    if (this.allowedTables == null) {
      return true;
    }
    if (canonicalName == null) {
      return false;
    }
    String genericName = canonicalName.toLowerCase();

    for (Name name : this.allowedTables) {
      if (name.isQuoted()) {
        if (canonicalName.equals(name.getName())) {
          this.acceptedTables.add(name);
          return true;
        }
      } else {
        if (genericName.equals(name.getName())) {
          this.acceptedTables.add(name);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean declared() {
    return this.allowedTables != null;
  }

  public int size() {
    return this.allowedTables == null ? 0 : this.allowedTables.size();
  }

  public List<Name> listNotAccepted() {
    if (this.allowedTables == null) {
      return new ArrayList<>();
    }

    List<Name> diff = new ArrayList<>(this.allowedTables);
    diff.removeAll(this.acceptedTables);
    return diff;
  }

  public String render() {
    return this.allowedTables == null ? ""
        : this.allowedTables.stream().map(n -> n.toString()).collect(Collectors.joining(","));
  }

}
