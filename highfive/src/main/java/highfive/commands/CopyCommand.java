package highfive.commands;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import highfive.exceptions.CouldNotCopyDataException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.Identifier;
import highfive.model.Table;

public class CopyCommand extends DualDataSourceCommand {

  private static final int MAX_BATCH_EXCEPTIONS_TO_DISPLAY = 3;

  public CopyCommand(final String sourceDatasourceName, final String destDatasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("Copy Data", sourceDatasourceName, destDatasourceName);
  }

  @Override
  public void execute()
      throws SQLException, InvalidSchemaException, CouldNotCopyDataException, UnsupportedDatabaseTypeException {

    List<String> errors = new ArrayList<>();

    // 1. Match tables

    List<Identifier> stables = this.ds.getDialect().listTablesNames();
    Map<String, Identifier> sid = stables.stream().collect(Collectors.toMap(t -> t.getGenericName(), t -> t));

    List<Identifier> dtables = this.ds2.getDialect().listTablesNames();
    Map<String, Identifier> did = dtables.stream().collect(Collectors.toMap(t -> t.getGenericName(), t -> t));

    if (this.ds2.getReadOnly()) {
      errors.add("Could not copy data to the datasource '" + this.ds2.getName()
          + "' since it's configured as read-only. To copy data to it, add/set the property '" + this.ds2.getName()
          + ".readonly' to 'false'.");
    }

    // 3. Verify the destination tables are empty

    info("Destination Database - Initial Row Count:");
    boolean allEmpty = true;
    for (Identifier t : dtables) {
      String tid = this.ds2.getDialect().renderSQLTableIdentifier(t);
      String sql = "select count(*) from " + tid;
      try (PreparedStatement ps = this.ds2.getConnection().prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
        if (rs.next()) {
          long count = rs.getLong(1);
          if (count > 0) {
            allEmpty = false;
          }
          info("  " + t.getGenericName() + ": " + DF.format(count) + " rows" + (count == 0 ? "" : " -- not empty"));
        }
      }
    }
    if (!allEmpty) {
      errors
          .add("Some destination tables are not empty. " + "Please make sure they are empty before copying the data.");
    }

    info(" ");
    info("Preparing data copy:");

    class ColumnPair {
      Column source;
      Column dest;
    }

    class TablePair {
      Identifier source;
      Identifier dest;
      List<ColumnPair> columns = new ArrayList<>();
    }

    List<TablePair> pairs = new ArrayList<>();

    for (Iterator<String> it = sid.keySet().iterator(); it.hasNext();) {
      String gname = it.next();
      Identifier dt = did.get(gname);
      if (dt == null) {
        errors.add("Found table '" + gname + "' in the source database, but not in the destination database");
      } else {
        TablePair pair = new TablePair();
        pair.source = sid.get(gname);
        pair.dest = dt;
        pairs.add(pair);
        it.remove();
        did.remove(gname);
      }
    }

    if (!did.isEmpty()) {
      errors.add("Found table '" + did.keySet().iterator().next()
          + "' in the destination database, but not in the source database");
    }

    // 2. Match columns on each table

    for (TablePair pair : pairs) {
      Table st = this.ds.getDialect().getTableMetaData(pair.source);
      Map<String, Column> scols = st.getColumns().stream().collect(Collectors.toMap(c -> c.getName(), c -> c));
      Table dt = this.ds2.getDialect().getTableMetaData(pair.dest);
      Map<String, Column> dcols = dt.getColumns().stream().collect(Collectors.toMap(c -> c.getName(), c -> c));
      for (Iterator<String> it = scols.keySet().iterator(); it.hasNext();) {
        String name = it.next();
        Column sc = scols.get(name);
        Column dc = dcols.get(name);
        if (dc == null) {
          errors.add("Found column '" + name + "' in table '" + pair.source.getGenericName()
              + "' in the source database, but not in the destination database");
        } else {
          ColumnPair cpair = new ColumnPair();
          if (!sc.getSerializer().getClass().equals(dc.getSerializer().getClass())) {
            errors.add("Cannot copy data of column '" + name + "' in table '" + pair.source.getGenericName()
                + "': it has different data types: '" + sc.getSerializer().getName() + "' in the source database, and '"
                + dc.getSerializer().getName() + "' in the destination database");
          } else {
            cpair.source = sc;
            cpair.dest = dc;
            pair.columns.add(cpair);
            it.remove();
            dcols.remove(name);
          }
        }
      }
      if (!dcols.isEmpty()) {
        errors.add("Found column '" + dcols.keySet().iterator().next() + "' in table '" + pair.source.getGenericName()
            + "' in the destination database, but not in the source database");
      }

      for (ColumnPair cp : pair.columns) {
        if (!cp.source.getSerializer().getClass().equals(cp.dest.getSerializer().getClass())) {
          errors.add("Found column '" + dcols.keySet().iterator().next() + "' in table '" + pair.source.getGenericName()
              + "' in the destination database, but not in the source database");
        }
      }

    }

    // 3. Check that all validations passed

    if (!errors.isEmpty()) {
      errors.stream().forEach(e -> error("  - " + e));
      throw new CouldNotCopyDataException(
          "Cannot copy data due to validation errors (" + errors.size() + ") -- Copy aborted.");
    } else {
      info("  All validation passed.");
    }

    // 4. Copy the data

    info(" ");
    long grandTotalRows = 0;

    for (TablePair pair : pairs) {

      info("Copying table " + pair.source.renderSQL() + ":");

      String stid = this.ds.getDialect().renderSQLTableIdentifier(pair.source);
      String pkNames = pair.columns.stream().filter(c -> c.source.getPKPosition() != null)
          .sorted((a, b) -> a.source.getPKPosition().compareTo(b.source.getPKPosition()))
          .map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.source.getCanonicalName()))
          .collect(Collectors.joining(", "));
      String snames = pair.columns.stream()
          .map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.source.getCanonicalName()))
          .collect(Collectors.joining(", "));
      String select = "select " + snames + " from " + stid;

      String dtid = this.ds2.getDialect().renderSQLTableIdentifier(pair.dest);
      String dnames = pair.columns.stream()
          .map(c -> this.ds2.getDialect().escapeIdentifierAsNeeded(c.dest.getCanonicalName()))
          .collect(Collectors.joining(", "));
      String insert = "insert into " + dtid + " (" + dnames + ") values ("
          + pair.columns.stream().map(x -> "?").collect(Collectors.joining(", ")) + ")";

      this.ds.getConnection().setAutoCommit(true); // end the current transaction, if any

      this.ds.getConnection().setAutoCommit(this.ds.getSelectAutoCommit());

      try (PreparedStatement selectPS = this.ds.getConnection().prepareStatement(select, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);) {

        if (this.ds.getSelectFetchSize() != null) {
          selectPS.setFetchSize(this.ds.getSelectFetchSize());
        }

        try (
            PreparedStatement insertPS = this.ds2.getConnection().prepareStatement(insert, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = selectPS.executeQuery();) {
          int logCount = 0;
          long currentBatch = 0;
          int rowsCount = 0;
          while (rs.next()) {
            int col = 1;
            for (ColumnPair cp : pair.columns) {
              Column c = cp.source;
              try {
                c.getSerializer().read(rs, col);
              } catch (SQLException e) {
                error("The JDBC driver could not read the value of column '" + c.getCanonicalName() + "' on table '"
                    + pair.source.getCanonicalName() + "' as a '" + c.getSerializer().getName()
                    + "' value. The error happened in row #" + (DF.format(rowsCount) + 1)
                    + " when the table is sorted by the primary key (" + pkNames + ").");
                throw e;
              } catch (RuntimeException e) {
                error("Could not serialize the value for column '" + c.getCanonicalName() + "' on table '"
                    + pair.source.getCanonicalName() + "'. The error happened in row #" + (DF.format(rowsCount) + 1)
                    + " when the table is sorted by the primary key (" + pkNames + "). Is '"
                    + c.getSerializer().getClass().getSimpleName() + "' the correct serializer for this column?");
                throw e;
              }

              c.getSerializer().set(insertPS, col);

              col++;
            }

            insertPS.addBatch();
            currentBatch++;
            if (currentBatch >= this.ds2.getInsertBatchSize()) {
              insertPS.executeBatch();
              currentBatch = 0;
            }

            logCount++;
            rowsCount++;
            if (logCount >= 50000) {
              info("  " + DF.format(rowsCount) + " rows copied");
              logCount = 0;
            }
            if (ds2.getMaxRows() != null && rowsCount >= ds2.getMaxRows()) {
              info("  - Limit of " + DF.format(ds2.getMaxRows())
                  + " rows (max.rows) reached when copying this table -- moving on to the next table.");
              break;
            }
          }

          if (currentBatch > 0) {
            try {
              insertPS.executeBatch();
            } catch (SQLException e) {
              error("Could not copy data to table '" + pair.dest.getCanonicalName() + "'.");
              error("-- SQL insert statement: " + insert);
              SQLException oe = e;
              int cnt = 0;
              e = e.getNextException();
              while (e != null) {
                cnt++;
                if (cnt <= MAX_BATCH_EXCEPTIONS_TO_DISPLAY) {
                  e.printStackTrace();
                  e = e.getNextException();
                } else {
                  e = null;
                }
              }
              if (cnt > MAX_BATCH_EXCEPTIONS_TO_DISPLAY) {
                info("-- Abridged: showing only the first " + MAX_BATCH_EXCEPTIONS_TO_DISPLAY
                    + " exception(s) of the batch insert.");
              }
              throw oe;
            }
          }

          info("  " + DF.format(rowsCount) + " row(s) copied");
          grandTotalRows = grandTotalRows + rowsCount;

        }
      }

    }

    info("Copy complete -- Grand total of " + DF.format(grandTotalRows) + " row(s) copied");

  }

}
