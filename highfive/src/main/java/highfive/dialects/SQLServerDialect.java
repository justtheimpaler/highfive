package highfive.dialects;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.exceptions.UnsupportedSQLFeatureException;
import highfive.model.Column;
import highfive.model.DataSource;
import highfive.model.Dialect;
import highfive.model.Identifier;
import highfive.model.Serializer;
import highfive.model.Table;
import highfive.serializers.BigDecimalSerializer;
import highfive.serializers.ByteArraySerializer;
import highfive.serializers.DoubleSerializer;
import highfive.serializers.IntegerSerializer;
import highfive.serializers.LocalDateSerializer;
import highfive.serializers.LocalDateTimeSerializer;
import highfive.serializers.LocalTimeSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.OffsetDateTimeSerializer;
import highfive.serializers.StringSerializer;

public class SQLServerDialect extends Dialect {

  public SQLServerDialect(DataSource ds, Connection conn) {
    super(ds, conn);
  }

  @Override
  public String getName() {
    return "SQL Server";
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = new ArrayList<>();
    String sql = "select name from " + (ds.getCatalog() == null || ds.getCatalog().isEmpty() ? ""
        : this.escapeIdentifierAsNeeded(ds.getCatalog()) + ".") + "sys.tables where schema_name(schema_id) = ?";
//    System.out.println("sql=" + sql);
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
    List<Column> columns = new ArrayList<>();
    String cat = ds.getCatalog() == null || ds.getCatalog().isEmpty() ? ""
        : "" + escapeIdentifierAsNeeded(ds.getCatalog()) + ".";
    String sql = "select c.name, t.name, c.max_length, c.precision, c.scale, ix.key_ordinal\n" //
        + "from " + cat + "sys.columns c\n" //
        + "join " + cat + "sys.types t on c.user_type_id = t.user_type_id\n" //
        + "outer apply (\n" //
        + "  select ic.key_ordinal\n" //
        + "  from " + cat + "sys.index_columns ic\n" //
        + "  join " + cat + "sys.indexes i on ic.object_id = i.object_id and ic.index_id = i.index_id\n" //
        + "  where ic.object_id = c.object_id and ic.column_id = c.column_id and i.is_primary_key = 1\n" //
        + ") ix\n" //
        + "where c.object_id = object_id('" + cat + escapeIdentifierAsNeeded(ds.getSchema()) + "." + tn.renderSQL()
        + "')";
//    System.out.println("sql=" + sql);
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          int col = 1;
          String name = readString(rs, col++);
          if (ds.getColumnFilter().accepts(name)) {
            String type = readString(rs, col++);
            boolean unsigned = false;
            Integer len = readInt(rs, col++);
            BigInteger length = len == null ? null : BigInteger.valueOf(len.longValue());
            Integer precision = readInt(rs, col++);
            Integer scale = readInt(rs, col++);
            Integer pkPosition = readInt(rs, col++);
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

  private String renderType(String name, String type, BigInteger maxLength, Integer precision, Integer scale) {
    if ("varchar".equals(type) || "char".equals(type) || "nvarchar".equals(type) || "nchar".equals(type)
        || "text".equals(type) || "ntext".equals(type) || "sysname".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("decimal".equals(type) || "numeric".equals(type)) {
      return type + "(" + precision + ", " + scale + ")";
    } else if ("money".equals(type) || "smallmoney".equals(type)) {
      return type;
    } else if ("int".equals(type) || "bigint".equals(type) || "smallint".equals(type) || "tinyint".equals(type)
        || "bit".equals(type)) {
      return type;
    } else if ("float".equals(type) || "real".equals(type)) {
      if (precision == null) {
        return type;
      } else {
        return type + "(" + precision + ")";
      }
    } else if ("date".equals(type) || "datetime".equals(type) || "smalldatetime".equals(type)
        || "datetime2".equals(type) || "datetimeoffset".equals(type) || "time".equals(type)) {
      return type;
    } else if ("binary".equals(type) || "varbinary".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("hierarchyid".equals(type)) {
      return type;
    } else if ("timestamp".equals(type)) {
      return type;
    } else if ("uniqueidentifier".equals(type)) {
      return type;
    } else if ("geometry".equals(type) || "geography".equals(type)) {
      return type;
    } else if ("xml".equals(type)) {
      return type;
    } else if ("image".equals(type)) {
      return type;
    }
    return type + "(" + maxLength + " | " + precision + ", " + scale + ")";
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    if ("varchar".equals(type) || "char".equals(type) || "nvarchar".equals(type) || "nchar".equals(type)
        || "text".equals(type) || "ntext".equals(type) || "sysname".equals(type)) {
      return new StringSerializer();
    } else if ("decimal".equals(type) || "numeric".equals(type)) {
      return new BigDecimalSerializer();
    } else if ("money".equals(type) || "smallmoney".equals(type)) {
      return new BigDecimalSerializer();
    } else if ("int".equals(type) || "smallint".equals(type) || "tinyint".equals(type) || "bit".equals(type)) {
      return new IntegerSerializer();
    } else if ("bigint".equals(type)) {
      return new LongSerializer();
    } else if ("float".equals(type) || "real".equals(type)) {
      return new DoubleSerializer();
    } else if ("date".equals(type)) {
      return new LocalDateSerializer();
    } else if ("datetime".equals(type) || "smalldatetime".equals(type) || "datetime2".equals(type)) {
      return new LocalDateTimeSerializer();
    } else if ("datetimeoffset".equals(type)) {
      return new OffsetDateTimeSerializer();
    } else if ("time".equals(type)) {
      return new LocalTimeSerializer();
    } else if ("binary".equals(type) || "varbinary".equals(type)) {
      return new ByteArraySerializer();
    } else if ("hierarchyid".equals(type)) {
      return null;
    } else if ("timestamp".equals(type)) {
      return null;
    } else if ("image".equals(type)) {
      return new ByteArraySerializer();
    } else if ("uniqueidentifier".equals(type)) {
      return new ByteArraySerializer();
    } else if ("geometry".equals(type) || "geography".equals(type)) {
      return null;
    } else if ("xml".equals(type)) {
      return new StringSerializer();
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

  private static Set<String> MUST_ESCAPE = new HashSet<>();
  static {
    MUST_ESCAPE.add("CASE");
    MUST_ESCAPE.add("case");
  }

  @Override
  public String escapeIdentifierAsNeeded(final String canonicalName) {
    if (canonicalName.matches("^[A-Za-z0-9_]+$") && !MUST_ESCAPE.contains(canonicalName)) {
      return canonicalName;
    } else {
      return "\"" + canonicalName.replace("\"", "\"\"").replace("'", "''") + "\"";
    }
  }

  @Override
  public String renderSQLTableIdentifier(Identifier table) {
    String catalog = ds.getCatalog();
    String schema = ds.getSchema();
    String cat = catalog == null || catalog.isEmpty() ? "" : escapeIdentifierAsNeeded(catalog) + ".";
    return cat + (schema == null ? "" : escapeIdentifierAsNeeded(schema) + ".") + table.renderSQL();
  }

  @Override
  public String renderHeadLimit(Long limit) {
    return limit == null ? "" : (" top " + limit);
  }

  @Override
  public String renderTailLimit(Long limit) {
    return "";
  }

  @Override
  public Boolean getDefaultAutoCommit() {
    return true;
  }

  @Override
  public String renderNullsOrdering(boolean nullsFirst) throws UnsupportedSQLFeatureException {
    throw new UnsupportedSQLFeatureException(
        "SQL Server does not implement NULLS FIRST or NULLS LAST in the ORDER BY clause.");
  }

}
