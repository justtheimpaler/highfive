package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import highfive.exceptions.CouldNotCopyDataException;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.Identifier;
import highfive.model.Table;
import highfive.model.TableHashingMember;
import highfive.model.TableHashingOrdering;

public class HashDupesCommand extends DataSourceCommand {

  public HashDupesCommand(String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("HashDupes", datasourceName);
  }

  @Override
  public void execute() throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      NoSuchAlgorithmException, CouldNotHashException, IOException, InvalidHashFileException, CouldNotCopyDataException,
      InvalidConfigurationException {

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();

    // 1. Validate tables

    if (!this.ds.getTableFilter().allTablesFound()) {
      throw new CouldNotHashException("Could not find the following tables declared in the property '"
          + this.ds.getName() + ".table.filter': " + this.ds.getTableFilter().listNotAccepted().stream()
              .map(n -> n.toString()).collect(Collectors.joining(", ")));

    }

    int tablesWithDupes = 0;
    int failedTables = 0;

    info(" ");

    for (Identifier tn : tableNames) {

      this.ds.getConnection().setAutoCommit(true); // end the current transaction, if any
      this.ds.getConnection().setAutoCommit(this.ds.getSelectAutoCommit());

      String tid = this.ds.getDialect().renderSQLTableIdentifier(tn);
      Table t = this.ds.getDialect().getTableMetaData(tn);
      String sql = "select count(*) as dupes, sum(cnt) as occurrences from (select count(*) as cnt from " + tid
          + " group by " + getOrderingColumns(t) + " having count(*) > 1) x";

      try (Statement st = this.ds.getConnection().createStatement(); ResultSet rs = st.executeQuery(sql);) {
        if (rs.next()) {
          long dupes = rs.getLong(1);
          long occurrences = rs.getLong(2);
          if (dupes > 0) {
            info("Table '" + tid + "' has " + dupes + " dupes with a total of " + occurrences + " occurrences.");
            tablesWithDupes++;
          } else {
            info("Table '" + tid + "' has no dupes -- OK");
          }
        } else {
          info("Could not count dupes for table '" + tid + "'.");
          failedTables++;
        }
      }

    }

    if (tablesWithDupes == 0) {
      if (failedTables == 0) {
        info("Validation succeeded -- No dupes found in any table.");
      } else {
        info("Validation failed -- No dupes found in " + tableNames.size() + " table(s), but " + failedTables
            + " table(s) could not be inspected.");
      }
    } else {
      info("Validation failed -- Dupes were found in " + tablesWithDupes + " out of " + tableNames.size() + " tables.");
    }

  }

  private String getOrderingColumns(Table t) throws CouldNotHashException {
    String orderingColumns = null;
    TableHashingOrdering tho = this.ds.getHashingOrderings().get(t.getIdentifier().getGenericName());
    if (tho != null) {

      try {
        tho.validate(this.ds, t);
      } catch (InvalidConfigurationException e) {
        throw new CouldNotHashException(e.getMessage());
      }

      Collection<TableHashingMember> thms = tho.getMembers().values();

      if (thms.isEmpty()) { // declared: *

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Column c : t.getColumns()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          String cm = this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName());
          sb.append(cm);
        }
        orderingColumns = sb.toString();

      } else { // declared: columns

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (TableHashingMember m : thms) {
          Column col = t.findColumn(m.getGenericColumnName());
          if (col == null) {
            throw new CouldNotHashException("Could not find column '" + m.getGenericColumnName()
                + "' specified in the hashing ordering in the table '" + t.getIdentifier().getCanonicalName() + "'.");
          }
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }

          String cm = this.ds.getDialect().escapeIdentifierAsNeeded(m.getGenericColumnName());
          sb.append(cm);
        }
        orderingColumns = sb.toString();

      }

    } else { // primary key

      List<Column> pkColumns = t.getPKColumns();
      if (pkColumns.isEmpty()) {
        throw new CouldNotHashException(
            "The schema is not supported since the table '" + t.getIdentifier().getGenericName()
                + "' has no primary key and no hashing ordering was declared using the property '" + this.ds.getName()
                + ".hashing.ordering'.");
      }
      orderingColumns = pkColumns.stream().map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName()))
          .collect(Collectors.joining(", "));

    }
    return orderingColumns;
  }

}
