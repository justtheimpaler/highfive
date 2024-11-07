package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.exceptions.UnsupportedSQLFeatureException;
import highfive.model.Column;
import highfive.model.HashFile;
import highfive.model.Hasher;
import highfive.model.Identifier;
import highfive.model.Table;
import highfive.model.TableHashingMember;
import highfive.model.TableHashingOrdering;
import highfive.utils.Utl;

public abstract class GenericHashCommand extends DataSourceCommand {

  protected HashFile hashFile;

  public GenericHashCommand(final String commandName, final String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super(commandName, datasourceName);
  }

  protected void hash()
      throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException, CouldNotHashException,
      NoSuchAlgorithmException, InvalidHashFileException, InvalidConfigurationException, IOException {

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();

    // 1. Validate tables

    this.hashFile = new HashFile();

    if (!this.ds.getTableFilter().allTablesFound()) {
      throw new CouldNotHashException("Could not find the following tables declared in the property '"
          + this.ds.getName() + ".table.filter': " + this.ds.getTableFilter().listNotAccepted().stream()
              .map(n -> n.toString()).collect(Collectors.joining(", ")));

    }

    List<Table> tables = new ArrayList<>();
    for (Identifier tn : tableNames) {
      Table t = this.ds.getDialect().getTableMetaData(tn);
      tables.add(t);
      String ro = renderOrdering(t);
      if (ro == null) {
        throw new CouldNotHashException(
            "  - Table " + tn.getCanonicalName() + " does not have a sorting order for hashing. "
                + "Add a primary key or declare a unique criteria for ordering using the property '" + this.ds.getName()
                + ".hashing.ordering'.");
      }
    }

    // 2. Display Row Count

    displayRowCount(tables);

    // 3. Check Hashing Preconditions

    checkIfHashingAndCopyingIsSupported(tables);

    // 4. Hash the schema

    info(" ");
    info("Hashing:");
    for (Table t : tables) {
      Identifier tn = t.getIdentifier();

      info("  Hashing table: " + tn.renderSQL());
      String names = t.getColumns().stream()
          .map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName()))
          .collect(Collectors.joining(", "));

      String selectOrdering = renderOrdering(t);

      String tid = this.ds.getDialect().renderSQLTableIdentifier(tn);
      String sql = "select" + this.ds.getDialect().renderHeadLimit(ds.getMaxRows()) + " " + names + " from " + tid
          + " order by " + selectOrdering + this.ds.getDialect().renderTailLimit(ds.getMaxRows());
      if (this.ds.getLogSQL()) {
        info("    * sql: " + sql);
      }
      Hasher h = new Hasher();

      this.ds.getConnection().setAutoCommit(this.ds.getSelectAutoCommit());

      try (
          PreparedStatement ps = this.ds.getConnection().prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY);
          ResultSet rs = ps.executeQuery();) {
        int logCount = 0;
        int rowsCount = 0;
        while (rs.next()) {
          int col = 1;
          byte[] bytes = null;
          boolean first = true;
          for (Column c : t.getColumns()) {
            try {
              bytes = c.getSerializer().read(rs, col++);
              if (this.ds.getLogHashingValues()) {
                String bullet = first ? "*" : " ";
                first = false;
                info("    " + bullet + " " + c.getName() + ": '" + c.getSerializer().getValue() + "' - encoded: "
                    + Utl.toHex(bytes));
              }
            } catch (SQLException e) {
              error("The JDBC driver could not read the value of column '" + c.getCanonicalName() + "' on table '"
                  + tn.getCanonicalName() + "' as a '" + c.getSerializer().getName()
                  + "' value. The error happened in row #" + DF.format(rowsCount + 1)
                  + " when the table is sorted by the columns: " + selectOrdering + ".");
              throw e;
            } catch (RuntimeException e) {
              error("Could not serialize the value for column '" + c.getCanonicalName() + "' on table '"
                  + tn.getCanonicalName() + "'. The error happened in row #" + DF.format(rowsCount + 1)
                  + " when the table is sorted by the columns: " + selectOrdering + ". Is '"
                  + c.getSerializer().getClass().getSimpleName() + "' the correct serializer for this column?");
              throw e;
            }
            h.apply(bytes);
          }
          logCount++;
          rowsCount++;
          if (logCount >= 100000) {
            info("    " + DF.format(rowsCount) + " rows read");
            logCount = 0;
          }
          if (ds.getMaxRows() != null && rowsCount >= ds.getMaxRows()) {
            info("    - Limit of " + ds.getMaxRows()
                + " rows (max.rows) reached when reading this table -- moving on to the next table.");
            break;
          }
        }
        byte[] tableHash = h.close();
        hashFile.add(Utl.toHex(tableHash), tn.getGenericName());
        info("    " + DF.format(rowsCount) + " row(s) read");
      }

    }

    hashFile.saveTo(this.ds.getHashFileName());
    info("  Data hashes generated to: " + this.ds.getHashFileName());

  }

  private String renderOrdering(Table t) throws CouldNotHashException {
    String selectOrdering = null;
    TableHashingOrdering tho = this.ds.getHashingOrderings().get(t.getIdentifier().getGenericName());
    if (tho != null) {

      try {
        tho.validate(this.ds, t);
      } catch (InvalidConfigurationException e) {
        throw new CouldNotHashException(e.getMessage());
      }

      Collection<TableHashingMember> thms = tho.getMembers().values();

      if (thms.isEmpty()) { // declared: *

        selectOrdering = t.getColumns().stream().map(c -> c.getName()).collect(Collectors.joining(", "));

      } else { // declared: columns

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (TableHashingMember m : thms) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(this.ds.getDialect().escapeIdentifierAsNeeded(m.getGenericColumnName()));
          if (!m.isAscending()) {
            sb.append(" desc");
          }
          if (m.getNullsFirst() != null) {
            try {
              sb.append(this.ds.getDialect().renderNullsOrdering(m.getNullsFirst()));
            } catch (UnsupportedSQLFeatureException e) {
              throw new CouldNotHashException(
                  "The schema is not supported since the table '" + t.getIdentifier().getGenericName()
                      + "' ordering includes NULLS FIRST or NULLS LAST and this is not supported by this database.");
            }
          }
        }
        selectOrdering = sb.toString();

      }

    } else {
      List<Column> pkColumns = t.getColumns().stream().filter(c -> c.getPKPosition() != null)
          .collect(Collectors.toList());
      if (pkColumns.isEmpty()) {
        throw new CouldNotHashException(
            "The schema is not supported since the table '" + t.getIdentifier().getGenericName()
                + "' has no primary key and no hashing ordering was declared using the property '" + this.ds.getName()
                + ".hashing.ordering'.");
      }
      selectOrdering = t.getColumns().stream().filter(c -> c.getPKPosition() != null)
          .sorted((a, b) -> a.getPKPosition().compareTo(b.getPKPosition()))
          .map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName()))
          .collect(Collectors.joining(", "));

    }
    return selectOrdering;
  }

}
