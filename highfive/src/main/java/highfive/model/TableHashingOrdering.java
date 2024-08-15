package highfive.model;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.UnsupportedSQLFeatureException;

public class TableHashingOrdering {

  private String tableName;
  private LinkedHashMap<String, TableHashingMember> members;

  public TableHashingOrdering(String tableName) {
    this.tableName = tableName;
    this.members = new LinkedHashMap<>();
  }

  public void validate(final DataSource ds, Table t) throws InvalidConfigurationException {
    Set<String> existingColumns = t.getColumns().stream().map(c -> c.getName()).collect(Collectors.toSet());
    for (String c : members.keySet()) {
      if (!existingColumns.contains(c)) {
        throw new InvalidConfigurationException("Invalid hashing ordering for table '" + this.tableName
            + "' declared in the property '" + ds.getName() + ".hashing.ordering': column '" + c + "' does not exist.");
      }
    }
    for (TableHashingMember m : members.values()) {
      if (m.getNullsFirst() != null) {
        try {
          ds.getDialect().renderNullsOrdering(m.getNullsFirst());
        } catch (UnsupportedSQLFeatureException e) {
          throw new InvalidConfigurationException(
              "Invalid hashing ordering for table '" + this.tableName + "' declared in the property '" + ds.getName()
                  + ".hashing.ordering': NULLS FIRST (nf) and NULLS LAST (nl) are not supported by this database.");
        }
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public LinkedHashMap<String, TableHashingMember> getMembers() {
    return members;
  }

  public boolean usesAllColumns() {
    return this.members.isEmpty();
  }

}
