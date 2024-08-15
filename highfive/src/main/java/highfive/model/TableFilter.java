package highfive.model;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import highfive.utils.SetUtl;

public class TableFilter implements OptionalProperty {

  private TreeSet<String> allowedTables;
  private Set<String> acceptedTables;

  public TableFilter(Set<String> allowedTables) {
    if (allowedTables == null || allowedTables.isEmpty()) {
      this.allowedTables = null;
    } else {
      this.allowedTables = new TreeSet<>(allowedTables.stream().map(n -> n.toLowerCase()).collect(Collectors.toSet()));
    }
    this.reset();
  }

  public void reset() {
    this.acceptedTables = new HashSet<>();
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
    boolean accepted = this.allowedTables.contains(genericName);
    if (accepted) {
      this.acceptedTables.add(genericName);
    }
    return accepted;
  }

  @Override
  public boolean declared() {
    return this.allowedTables != null;
  }

  public int size() {
    return this.allowedTables == null ? 0 : this.allowedTables.size();
  }

  public Set<String> listNotAccepted() {
    if (this.allowedTables == null) {
      return new HashSet<>();
    }
//    System.out.println("### allowed: " + this.allowedTables);
//    System.out.println("### accepted: " + this.acceptedTables);
//    System.out.println("### diff: " + SetUtl.difference(this.allowedTables, this.acceptedTables));
    return new TreeSet<>(SetUtl.difference(this.allowedTables, this.acceptedTables));
  }

  public String render() {
    return this.allowedTables == null ? "" : this.allowedTables.stream().collect(Collectors.joining(","));
  }

}
