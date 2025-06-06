package highfive.dialects;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.DataSource;
import highfive.model.Dialect;
import highfive.model.Identifier;
import highfive.model.PKColumn;
import highfive.model.Serializer;
import highfive.model.Table;
import highfive.serializers.BigDecimalSerializer;
import highfive.serializers.BigIntegerSerializer;
import highfive.serializers.ByteArraySerializer;
import highfive.serializers.DoubleSerializer;
import highfive.serializers.IntegerSerializer;
import highfive.serializers.LocalDateTimeSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.StringSerializer;
import highfive.serializers.ZonedDateTimeSerializer;

public class OracleDialect extends Dialect {

  public OracleDialect(DataSource ds, Connection conn) {
    super(ds, conn);
  }

  @Override
  public String getName() {
    return "Oracle";
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = new ArrayList<>();
    String sql = "select table_name from all_tables where owner = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      ps.setString(1, ds.getSchema());
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          String table = rs.getString(1);
          if (rs.wasNull()) {
            throw new InvalidSchemaException("The schema includes a table with no name.");
          }
          if (ds.getTableFilter().accepts(table)) {
            tables.add(new Identifier(table, ds.getRemoveTablePrefix(), this));
          }
        }
      }
    }
    return tables;
  }

  @Override
  public Table getTableMetaData(Identifier tn) throws SQLException, UnsupportedDatabaseTypeException {

    List<PKColumn> pkColumns = getPrimaryKeyColumns(conn, ds.getSchema(), tn);
    Map<String, Integer> pkByName = pkColumns.stream()
        .collect(Collectors.toMap(x -> x.getCanonicalName(), x -> x.getPosition()));

    List<Column> columns = new ArrayList<>();
    String sql = "select column_name, data_type, data_length, data_precision, data_scale "
        + "from all_tab_columns where owner = ? and table_name = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      ps.setString(1, ds.getSchema());
      ps.setString(2, tn.getCanonicalName());
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          int col = 1;
          String name = readString(rs, col++);
          if (ds.getColumnFilter().accepts(name)) {
            String type = readString(rs, col++);
            boolean unsigned = false;
            Integer len = readInt(rs, col++);
            BigInteger length = len == null ? null : BigInteger.valueOf(len);
            Integer precision = readInt(rs, col++);
            Integer scale = readInt(rs, col++);
            Integer pkPosition = pkByName.get(name);
            String renderedType = renderType(name, type, length, precision, scale);
            Serializer<?> serializer = super.getSerializer(renderedType, tn, name, type, unsigned, length, precision,
                scale);
            Column c = new Column(name, type, length, precision, scale, renderedType, pkPosition, serializer);
            columns.add(c);
          }
        }
      }
    }
    return new Table(tn, columns);
  }

  private List<PKColumn> getPrimaryKeyColumns(Connection conn, String schema, Identifier table) throws SQLException {
    List<PKColumn> pk = new ArrayList<>();
    String sql = "select c.column_name, c.position " + "from all_cons_columns c "
        + "join all_constraints x on x.constraint_name = c.constraint_name and x.owner = c.owner "
        + "where x.owner = ? and x.table_name = ? and x.constraint_type = 'P'";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      ps.setString(1, schema);
      ps.setString(2, table.getCanonicalName());
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          String name = readString(rs, 1);
          Integer position = readInt(rs, 2);
          PKColumn pkc = new PKColumn(position, name);
          pk.add(pkc);
        }
      }
    }
    return pk;
  }

  private String renderType(String name, String type, BigInteger length, Integer precision, Integer scale) {
    if (type == null) {
      return "N/A";
    } else if ("CHAR".equals(type) || "VARCHAR2".equals(type) || "NCHAR".equals(type) || "NVARCHAR2".equals(type)) {
      return type + "(" + length + ")";
    } else if ("CLOB".equals(type) || "NCLOB".equals(type)) {
      return type + "(" + length + ")";
    } else if ("NUMBER".equals(type)) {
      if (precision == null) {
        return type;
      } else {
        return type + "(" + precision + ", " + scale + ")";
      }
    } else if ("FLOAT".equals(type) || "BINARY_FLOAT".equals(type) || "BINARY_DOUBLE".equals(type)) {
      return type;
    } else if ("DATE".equals(type) || type.startsWith("TIMESTAMP")) {
      return type;
    } else if ("RAW".equals(type)) {
      return type + "(" + length + ")";
    } else if ("BLOB".equals(type) || "LONG RAW".equals(type)) {
      return type + "(" + length + ")";
    } else if (type.startsWith("INTERVAL")) {
      return type;
    } else {
      return type;
    }
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    if (type == null) {
      throw new UnsupportedDatabaseTypeException(
          "Unsupported column type for column " + name + " in table " + table + ": " + type);
    } else if ("CHAR".equals(type) || "VARCHAR2".equals(type) || "NCHAR".equals(type) || "NVARCHAR2".equals(type)) {
      return new StringSerializer();
    } else if ("CLOB".equals(type) || "NCLOB".equals(type)) {
      return new StringSerializer();
    } else if ("NUMBER".equals(type)) {
      if (precision == null) {
        return new BigIntegerSerializer();
      } else if (scale == null | scale.equals(0)) {
        if (precision <= 9) {
          return new IntegerSerializer();
        } else if (precision <= 18) {
          return new LongSerializer();
        } else {
          return new BigIntegerSerializer();
        }
      } else {
        return new BigDecimalSerializer();
      }
    } else if ("FLOAT".equals(type) || "BINARY_FLOAT".equals(type) || "BINARY_DOUBLE".equals(type)) {
      return new DoubleSerializer();
    } else if ("DATE".equals(type)) {
      return new LocalDateTimeSerializer();
    } else if (type.matches("^TIMESTAMP\\(\\d+\\)$")) {
      return new LocalDateTimeSerializer();
    } else if (type.matches("^TIMESTAMP\\(\\d+\\) WITH TIME ZONE$")) {
      return new ZonedDateTimeSerializer();
    } else if (type.matches("^TIMESTAMP\\(\\d+\\) WITH LOCAL TIME ZONE$")) {
      return new ZonedDateTimeSerializer();
    } else if ("RAW".equals(type) || "BLOB".equals(type) || "LONG RAW".equals(type)) {
      return new ByteArraySerializer();
    }
    return null;
  }

  private String readString(ResultSet rs, int ordinal) throws SQLException {
    String v = rs.getString(ordinal);
    if (rs.wasNull()) {
      return null;
    }
    return v;
  }

  private Integer readInt(ResultSet rs, int ordinal) throws SQLException {
    Integer v = rs.getInt(ordinal);
    if (rs.wasNull()) {
      return null;
    }
    return v;
  }

  @SuppressWarnings("unused")
  private <T> T readObject(ResultSet rs, int ordinal, Class<T> cls) throws SQLException {
    T v = rs.getObject(ordinal, cls);
    if (rs.wasNull()) {
      return null;
    }
    return v;
  }

  @Override
  public String escapeIdentifierAsNeeded(String canonicalName) {
    if (canonicalName.matches("^[A-Z0-9_]+$")) {
      return canonicalName;
    } else {
      return "\"" + canonicalName.replace("\"", "\"\"") + "\"";
    }
  }

  @Override
  public String addCollation(String columnCanonicalName, String collation) {
    return columnCanonicalName + " collate " + collation;
  }

  @Override
  public String renderSQLTableIdentifier(Identifier table) {
    return (ds.getSchema() == null ? "" : escapeIdentifierAsNeeded(ds.getSchema()) + ".") + table.renderSQL();
  }

  @Override
  public String renderHeadLimit(Long limit) {
    return "";
  }

  @Override
  public String renderTailLimit(Long limit) {
    return limit == null ? "" : (" fetch next " + limit + " rows only");
  }

  @Override
  public Boolean getDefaultAutoCommit() {
    return true;
  }

  @Override
  public String renderNullsOrdering(boolean nullsFirst) {
    return nullsFirst ? " nulls first" : " nulls last";
  }

}
