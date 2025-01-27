package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import highfive.commands.consumer.HashConsumer;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.exceptions.UnsupportedSQLFeatureException;
import highfive.model.Column;
import highfive.model.Hasher;
import highfive.model.Identifier;
import highfive.model.Table;
import highfive.model.TableHashingMember;
import highfive.model.TableHashingOrdering;

public abstract class GenericHashCommand extends DataSourceCommand {

  private static final int MAX_ORDERING_ERRORS = 3;

//  @Deprecated
//  protected HashFile hashFile;

  public GenericHashCommand(final String commandName, final String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super(commandName, datasourceName);
  }

  protected void hashOneSchema(final HashConsumer hc)
      throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException, CouldNotHashException,
      NoSuchAlgorithmException, InvalidHashFileException, InvalidConfigurationException, IOException {

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();
//    info("hashDumpConfig=" + hashDumpConfig);
//    if (hashDumpConfig == null) {

    // 1. Validate tables

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
      hashOneTable(t, hc);
    }

    info("  Data hashes generated to: " + this.ds.getHashFileName());

  }

  protected Identifier findTable(String tableName, List<Identifier> tableNames) {
    for (Identifier tn : tableNames) {
      if (tn.getGenericName().equals(tableName)) {
        return tn;
      }
    }
    return null;
  }

  protected void hashOneTable(Table t, HashConsumer consumer)
      throws CouldNotHashException, NoSuchAlgorithmException, SQLException {
    Identifier tn = t.getIdentifier();

    info("  Hashing table: " + tn.renderSQL());

    String names = t.getColumns().stream().map(c -> this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName()))
        .collect(Collectors.joining(", "));

    String selectOrdering = renderOrdering(t);
    RowComparator rowComparator = getRowComparator(t);

    String tid = this.ds.getDialect().renderSQLTableIdentifier(tn);
    String sql = "select" + this.ds.getDialect().renderHeadLimit(ds.getMaxRows()) + " " + names + " from " + tid
        + " order by " + selectOrdering + this.ds.getDialect().renderTailLimit(ds.getMaxRows());
    if (this.ds.getLogSQL()) {
      info("    * sql: " + sql);
    }
    Hasher h = new Hasher();

    consumer.initializeHasher(h);

    this.ds.getConnection().setAutoCommit(true); // end the current transaction, if any

    this.ds.getConnection().setAutoCommit(this.ds.getSelectAutoCommit());

    try (PreparedStatement ps = this.ds.getConnection().prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);) {

      if (this.ds.getSelectFetchSize() != null) {
        ps.setFetchSize(this.ds.getSelectFetchSize());
      }

      try (ResultSet rs = ps.executeQuery();) {
        int logCount = 0;
        int row = 0;
        int orderingErrors = 0;
        boolean active = true;

        while (rs.next() && active) {
          logCount++;
          row++;
          if (logCount >= 100000) {
            info("    " + DF.format(row) + " rows read");
            logCount = 0;
          }

          consumer.consumeValueHeader(row);
          int col = 1;
          byte[] bytes = null;
          for (Column c : t.getColumns()) {
            try {
              bytes = c.getSerializer().read(rs, col);
              Object v = c.getSerializer().getValue();
              rowComparator.setColumn(col, v);
            } catch (SQLException e) {
              error("The JDBC driver could not read the value of column '" + c.getCanonicalName() + "' on table '"
                  + tn.getCanonicalName() + "' as a '" + c.getSerializer().getName()
                  + "' value. The error happened in row #" + DF.format(row)
                  + " when the table is sorted by the columns: " + selectOrdering + ".");
              throw e;
            } catch (RuntimeException e) {
              error("Could not serialize the value for column '" + c.getCanonicalName() + "' on table '"
                  + tn.getCanonicalName() + "'. The error happened in row #" + DF.format(row)
                  + " when the table is sorted by the columns: " + selectOrdering + ". Is '"
                  + c.getSerializer().getClass().getSimpleName() + "' the correct serializer for this column?");
              throw e;
            }
            h.apply(bytes);

            consumer.consumeValue(row, c, bytes, h);
            col++;
          }
          if (ds.getMaxRows() != null && row >= ds.getMaxRows()) {
            info("    - Limit of " + ds.getMaxRows()
                + " rows (max.rows) reached when reading this table -- moving on to the next table.");
            break;
          }

          boolean hasValidOrdering = rowComparator.hasValidOrdering();
          if (!hasValidOrdering) {
            orderingErrors++;
            if (orderingErrors <= MAX_ORDERING_ERRORS) {
              error("Non-deterministic hashing ordering found in table '" + tn.getCanonicalName() + "' (#"
                  + orderingErrors + "); found at least two rows with the same value in the ordering columns ("
                  + rowComparator.getOrderingColumns().stream().collect(Collectors.joining(", "))
                  + "), but different values in the rest of the columns:");
              error(" * Row 1: " + rowComparator.renderPreviousRow());
              error(" * Row 2: " + rowComparator.renderCurrentRow());
            }
          }

          rowComparator.next();
          active = consumer.consumeRow(row, h);

        }

        if (orderingErrors > 0) {
          error("The hashing computation won't be predictable for the table '" + tn.getCanonicalName()
              + "' and can result in false positives or false negatives.");
        }
        if (orderingErrors > MAX_ORDERING_ERRORS) {
          error("A total of " + orderingErrors + " non-deterministic hashing ordering issues were found in table '"
              + tn.getCanonicalName() + "'; only the first " + MAX_ORDERING_ERRORS + " were displayed.");
        }

        consumer.consumeTable(t.getIdentifier().getGenericName(), orderingErrors > 0);

        info("    " + DF.format(row) + " row(s) read");

      } catch (Throwable e) {
        e.printStackTrace(System.out);
        info("    Failed to read table " + tn.getGenericName() + " -- skipped");
      }
    }
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

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Column c : t.getColumns()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          String cm = this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName());
          if (c.getSerializer().canUseACollation() && this.ds.getHashingCollation() != null) {
            cm = this.ds.getDialect().addCollation(cm, this.ds.getHashingCollation());
          }
          sb.append(cm);
        }
        selectOrdering = sb.toString();

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
          if (col.getSerializer().canUseACollation() && this.ds.getHashingCollation() != null) {
            cm = this.ds.getDialect().addCollation(cm, this.ds.getHashingCollation());
          }

          sb.append(cm);
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

    } else { // primary key

      List<Column> pkColumns = t.getPKColumns();
      if (pkColumns.isEmpty()) {
        throw new CouldNotHashException(
            "The schema is not supported since the table '" + t.getIdentifier().getGenericName()
                + "' has no primary key and no hashing ordering was declared using the property '" + this.ds.getName()
                + ".hashing.ordering'.");
      }

      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Column c : pkColumns) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        String cm = this.ds.getDialect().escapeIdentifierAsNeeded(c.getCanonicalName());
        if (c.getSerializer().canUseACollation() && this.ds.getHashingCollation() != null) {
          cm = this.ds.getDialect().addCollation(cm, this.ds.getHashingCollation());
        }
        sb.append(cm);
      }
      selectOrdering = sb.toString();

    }
    return selectOrdering;
  }

  private RowComparator getRowComparator(Table t) {

    Map<String, Integer> ordinals = new HashMap<>();
    int ordinal = 1;
    for (Column c : t.getColumns()) {
      ordinals.put(c.getCanonicalName(), ordinal);
      ordinal++;
    }

    List<String> columnNames = t.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList());
    List<Integer> orderingColumnsOrdinals;

    TableHashingOrdering tho = this.ds.getHashingOrderings().get(t.getIdentifier().getGenericName());
    if (tho == null) { // ordering not declared -- use the PK
      orderingColumnsOrdinals = t.getColumns().stream().filter(c -> c.getPKPosition() != null)
          .map(c -> ordinals.get(c.getCanonicalName())).collect(Collectors.toList());
    } else {
      Collection<TableHashingMember> thms = tho.getMembers().values();
      if (thms.isEmpty()) { // ordering declared as: *
        orderingColumnsOrdinals = IntStream.range(1, t.getColumns().size() + 1).boxed().collect(Collectors.toList());
      } else { // ordering declared as a list of columns
        orderingColumnsOrdinals = new ArrayList<>();
        for (TableHashingMember m : thms) {
          Column col = t.findColumn(m.getGenericColumnName());
          orderingColumnsOrdinals.add(ordinals.get(col.getCanonicalName()));
        }
      }
    }

    return new RowComparator(columnNames, orderingColumnsOrdinals);
  }

  private static class RowComparator {

    private int numberOfColumns;
    private String[] columnNames;

    private boolean firstRow;
    private boolean[] usedForOrdering;
    private String[] previousRow;
    private String[] currentRow;

    private List<String> orderingColumns;

    public RowComparator(List<String> columnNames, List<Integer> orderingColumnsOrdinals) {

      this.numberOfColumns = columnNames.size();
      this.columnNames = columnNames.toArray(new String[0]);

      this.firstRow = true;
      this.usedForOrdering = new boolean[this.numberOfColumns];
      this.previousRow = new String[this.numberOfColumns];
      this.currentRow = new String[this.numberOfColumns];
      for (int i = 0; i < this.numberOfColumns; i++) {
        this.usedForOrdering[i] = false;
        this.previousRow[i] = null;
        this.currentRow[i] = null;
      }

      this.orderingColumns = orderingColumnsOrdinals.stream().map(o -> this.columnNames[o - 1])
          .collect(Collectors.toList());

      for (Integer ordinal : orderingColumnsOrdinals) {
        this.usedForOrdering[ordinal - 1] = true;
      }
    }

    public void setColumn(int ordinal, Object value) {
      this.currentRow[ordinal - 1] = value == null ? null : ("" + value);
    }

    public boolean hasValidOrdering() {
      if (this.firstRow) {
        this.firstRow = false;
        return true;
      } else {

        boolean equalOrdering = true;
        boolean equalRest = true;

        for (int i = 0; i < this.numberOfColumns; i++) {
          boolean indf = isNotDistinctFrom(this.currentRow[i], this.previousRow[i]);
//          System.out.println(">> comparing " + this.currentRow[i] + " and " + this.previousRow[i] + " --> " + indf);
          if (this.usedForOrdering[i]) {
            equalOrdering = equalOrdering && indf;
          } else {
            equalRest = equalRest && indf;
          }
        }

        if (equalOrdering) {
          return equalRest;
        } else {
          return true;
        }
      }
    }

    private boolean isNotDistinctFrom(String a, String b) {
      return a == null ? b == null : a.equals(b);
    }

    public void next() {
//      System.out.println("--next line");
      this.previousRow = this.currentRow;
      this.currentRow = new String[this.numberOfColumns];
    }

    public String renderCurrentRow() {
      return this.renderRow(this.currentRow);
    }

    public String renderPreviousRow() {
      return this.renderRow(this.previousRow);
    }

    private String renderRow(String[] values) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < this.numberOfColumns; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(this.columnNames[i] + "=" + values[i]);
      }
      return sb.toString();
    }

    public List<String> getOrderingColumns() {
      return orderingColumns;
    }

  }

}
