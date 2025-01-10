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
import highfive.exceptions.UnsupportedSQLFeatureException;
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
import highfive.serializers.LocalDateSerializer;
import highfive.serializers.LocalDateTimeSerializer;
import highfive.serializers.LocalTimeSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.OffsetDateTimeSerializer;
import highfive.serializers.StringSerializer;
import highfive.utils.Utl;

public class MySQLDialect extends Dialect {

  public MySQLDialect(DataSource ds, Connection conn) {
    super(ds, conn);
  }

  @Override
  public String getName() {
    return "MySQL";
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = new ArrayList<>();
    String sql = "select table_name from information_schema.tables where table_schema = ? and table_type = 'BASE TABLE'";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      String database = Utl.coalesce(ds.getCatalog(), ds.getSchema());
      ps.setString(1, database);
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

    List<PKColumn> pkColumns = getPrimaryKeyColumns(conn, ds.getCatalog(), ds.getSchema(), tn);
    Map<String, Integer> pkByName = pkColumns.stream()
        .collect(Collectors.toMap(x -> x.getCanonicalName(), x -> x.getPosition()));

    List<Column> columns = new ArrayList<>();
    String sql = "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, column_type "
        + "from information_schema.columns where table_schema = ? and table_name = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      String database = Utl.coalesce(ds.getCatalog(), ds.getSchema());
      ps.setString(1, database);
      ps.setString(2, tn.getCanonicalName());
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          int col = 1;
          String name = readString(rs, col++);
          if (ds.getColumnFilter().accepts(name)) {
            String type = readString(rs, col++);
            BigInteger maxLength = readObject(rs, col++, BigInteger.class);
            Integer precision = readInt(rs, col++);
            Integer scale = readInt(rs, col++);
            String columnType = readString(rs, col++);
            boolean unsigned = columnType != null && columnType.contains("unsigned");
            Integer pkPosition = pkByName.get(name);
            String renderedType = renderType(name, type, unsigned, maxLength, precision, scale);
            Serializer<?> serializer = super.getSerializer(renderedType, tn, name, type, unsigned, maxLength, precision,
                scale);
            Column c = new Column(name, type, maxLength, precision, scale, renderedType, pkPosition, serializer);
            columns.add(c);
          }
        }
      }
    }
    return new Table(tn, columns);
  }

  private String renderType(String name, String type, boolean unsigned, BigInteger length, Integer precision,
      Integer scale) {
    if ("char".equals(type) || "varchar".equals(type)) {
      return type + "(" + length + ")";
    } else if ("tinytext".equals(type) || "text".equals(type) || "mediumtext".equals(type) || "longtext".equals(type)) {
      return type;
    } else if ("tinyint".equals(type) || "smallint".equals(type) || "mediumint".equals(type) || "int".equals(type)
        || "bigint".equals(type)) {
      return type + (unsigned ? " unsigned" : "");
    } else if ("decimal".equals(type)) {
      return type + "(" + precision + ", " + scale + ")";
    } else if ("float".equals(type) || "double".equals(type)) {
      return type + (unsigned ? " unsigned" : "");
    } else if ("date".equals(type) || "datetime".equals(type) || "timestamp".equals(type) || "time".equals(type)
        || "year".equals(type)) {
      return type;
    } else if ("tinyblob".equals(type) || "blob".equals(type) || "mediumblob".equals(type) || "longblob".equals(type)) {
      return type;
    } else if ("enum".equals(type)) {
      return type;
    } else if ("set".equals(type)) {
      return type;
    } else {
      return type;
    }
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    if ("char".equals(type) || "varchar".equals(type)) {
      return new StringSerializer();
    } else if ("tinytext".equals(type) || "text".equals(type) || "mediumtext".equals(type) || "longtext".equals(type)) {
      return new StringSerializer();
    } else if ("tinyint".equals(type) || "smallint".equals(type) || "mediumint".equals(type)
        || "int".equals(type) && !unsigned) {
      return new IntegerSerializer();
    } else if ("int".equals(type) && unsigned || "bigint".equals(type) && !unsigned) {
      return new LongSerializer();
    } else if ("bigint".equals(type) && unsigned) {
      return new BigIntegerSerializer();
    } else if ("decimal".equals(type)) {
      return new BigDecimalSerializer();
    } else if ("float".equals(type) || "double".equals(type) && !unsigned) {
      return new DoubleSerializer();
    } else if ("double".equals(type) && unsigned) {
      return new BigDecimalSerializer();
    } else if ("date".equals(type)) {
      return new LocalDateSerializer();
    } else if ("datetime".equals(type)) {
      return new LocalDateTimeSerializer();
    } else if ("timestamp".equals(type)) {
      return new OffsetDateTimeSerializer();
    } else if ("time".equals(type)) {
      return new LocalTimeSerializer();
    } else if ("year".equals(type)) {
      return new IntegerSerializer();
    } else if ("tinyblob".equals(type) || "blob".equals(type) || "mediumblob".equals(type) || "longblob".equals(type)) {
      return new ByteArraySerializer();
    }
    return null;
  }

  private List<PKColumn> getPrimaryKeyColumns(Connection conn, String catalog, String schema, Identifier table)
      throws SQLException {
    String sql = "select column_name, ordinal_position " + "from information_schema.key_column_usage "
        + "where table_schema = ? and constraint_name = 'PRIMARY' and table_name = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      String database = Utl.coalesce(catalog, schema);
      ps.setString(1, database);
      ps.setString(2, table.getCanonicalName());
      try (ResultSet rs = ps.executeQuery();) {
        List<PKColumn> pk = new ArrayList<>();
        while (rs.next()) {
          String name = readString(rs, 1);
          Integer position = readInt(rs, 2);
          PKColumn c = new PKColumn(position, name);
          pk.add(c);
        }
        return pk;
      }
    }
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

  private <T> T readObject(ResultSet rs, int ordinal, Class<T> cls) throws SQLException {
    T v = rs.getObject(ordinal, cls);
    if (rs.wasNull()) {
      return null;
    }
    return v;
  }

  @Override
  public String escapeIdentifierAsNeeded(String canonicalName) {
    // MySQL case sensitiveness is heavily dependent on the underlying OS --
    // escape always
    return "`" + canonicalName + "`";
  }

  @Override
  public String addCollation(String columnCanonicalName, String collation) {
    return columnCanonicalName + " collate " + collation;
  }

  @Override
  public String renderSQLTableIdentifier(Identifier table) {
    String database = Utl.coalesce(ds.getCatalog(), ds.getSchema());
    return (database == null ? "" : escapeIdentifierAsNeeded(database) + ".") + table.renderSQL();
  }

  @Override
  public String renderHeadLimit(Long limit) {
    return "";
  }

  @Override
  public String renderTailLimit(Long limit) {
    return limit == null ? "" : (" limit " + limit);
  }

  @Override
  public Boolean getDefaultAutoCommit() {
    return true;
  }

  @Override
  public String renderNullsOrdering(boolean nullsFirst) throws UnsupportedSQLFeatureException {
    throw new UnsupportedSQLFeatureException(
        "MySQL does not implement NULLS FIRST or NULLS LAST in the ORDER BY clause.");
  }

}
