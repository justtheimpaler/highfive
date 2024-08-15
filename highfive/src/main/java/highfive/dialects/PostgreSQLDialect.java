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
import highfive.serializers.BooleanSerializer;
import highfive.serializers.ByteArraySerializer;
import highfive.serializers.DoubleSerializer;
import highfive.serializers.IntegerSerializer;
import highfive.serializers.LocalDateSerializer;
import highfive.serializers.LocalDateTimeSerializer;
import highfive.serializers.LocalTimeSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.OffsetDateTimeSerializer;
import highfive.serializers.StringSerializer;

public class PostgreSQLDialect extends Dialect {

  public PostgreSQLDialect(DataSource ds, Connection conn) {
    super(ds, conn);
  }

  @Override
  public String getName() {
    return "PostgreSQL";
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = new ArrayList<>();
    String sql = "select table_name from information_schema.tables where table_schema = ?";
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
    String sql = "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale\n"
        + "from information_schema.columns where table_schema = ? and table_name = ?";
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
            BigInteger length = len == null ? null : BigInteger.valueOf(len.longValue());
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

// ASC/DESC is OK
// UNIQUE only
// b-tree only
// maybe: nulls first/last
// no functional indexes
// no partial indexes
// no collations

//  private List<Index> listNonFunctionalIndexes(Identifier table, boolean onlyUniqueIndexes) throws SQLException {
//    List<Index> ixs = new ArrayList<>();
//    String sql = "select it.indexrelid as ix_id, ins.nspname as ix_schema, i.relname as ix_name, it.indisunique\n"
//        + "from pg_index it\n" //
//        + "join pg_class i on i.oid = it.indexrelid\n" //
//        + "join pg_namespace ins on ins.oid = i.relnamespace\n" //
//        + "join pg_class t on t.oid = it.indrelid\n" //
//        + "join pg_namespace tns on tns.oid = t.relnamespace\n" //
//        + "where 0 <> all(it.indkey)" + (onlyUniqueIndexes ? " and it.indisunique" : "")
//        + " and tns.nspname = ? and t.relname = ?";
//    try (PreparedStatement ps = conn.prepareStatement(sql);) {
//      ps.setString(1, ds.getSchema());
//      ps.setString(2, table.getCanonicalName());
//      try (ResultSet rs = ps.executeQuery();) {
//        while (rs.next()) {
//          Index ix = new Index();
//          int col = 1;
//          ix.id = rs.getLong(col++);
//          ix.schema = rs.getString(col++);
//          ix.name = rs.getString(col++);
//          ix.unique = rs.getBoolean(col++);
//          ixs.add(ix);
//        }
//      }
//    }
//    return ixs;
//  }

//  private List<UniqueIndexMember> listIndexColumns(Index index) throws SQLException {
//    List<UniqueIndexMember> members = new ArrayList<>();
//    String sql = "select a.attname\n" //
//        + "from pg_index i\n" //
//        + "left join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) and a.attnum > 0\n" //
//        + "where i.indexrelid = ? \n" //
//        + "order by a.attnum";
//    try (PreparedStatement ps = conn.prepareStatement(sql);) {
//      ps.setLong(1, index.id);
//      try (ResultSet rs = ps.executeQuery();) {
//        while (rs.next()) {
//          String name = rs.getString(1);
//          Identifier id = new Identifier(name, null, this);
////          members.add(id);
//        }
//      }
//    }
//    return members;
//  }

//  class Index {
//    boolean unique;
//    long id;
//    String schema;
//    String name;
//  }

  private String renderType(String name, String type, BigInteger maxLength, Integer precision, Integer scale) {
    if ("character".equals(type) || "character varying".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("text".equals(type)) {
      return type;
    } else if ("smallint".equals(type) || "integer".equals(type) || "bigint".equals(type)) {
      return type;
    } else if ("numeric".equals(type)) {
      return type + "(" + precision + ", " + scale + ")";
    } else if ("real".equals(type) || "double precision".equals(type)) {
      return type;
    } else if ("date".equals(type) || "timestamp without time zone".equals(type)
        || "timestamp with time zone".equals(type) || "time without time zone".equals(type)
        || "time with time zone".equals(type) || "interval".equals(type)) {
      return type;
    } else if ("bytea".equals(type)) {
      return type;
    } else if ("boolean".equals(type)) {
      return type;
    } else {
      return type;
    }
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    if ("character".equals(type) || "character varying".equals(type) || "text".equals(type)) {
      return new StringSerializer();
    } else if ("smallint".equals(type) || "integer".equals(type)) {
      return new IntegerSerializer();
    } else if ("bigint".equals(type)) {
      return new LongSerializer();
    } else if ("numeric".equals(type)) {
      return new BigDecimalSerializer();
    } else if ("real".equals(type) || "double precision".equals(type)) {
      return new DoubleSerializer();

    } else if ("date".equals(type)) {
      return new LocalDateSerializer();
    } else if ("timestamp without time zone".equals(type)) {
      return new LocalDateTimeSerializer();
    } else if ("timestamp with time zone".equals(type)) {
      return new OffsetDateTimeSerializer();
    } else if ("time without time zone".equals(type)) {
      return new LocalTimeSerializer();
    } else if ("time with time zone".equals(type) || "interval".equals(type)) {
      throw new UnsupportedDatabaseTypeException(
          "Unsupported column type for column " + name + " in table " + table + ": " + type);
    } else if ("bytea".equals(type)) {
      return new ByteArraySerializer();
    } else if ("boolean".equals(type)) {
      return new BooleanSerializer();
    }
    return null;
  }

  private List<PKColumn> getPrimaryKeyColumns(Connection conn, String schema, Identifier table) throws SQLException {
    String sql = "select a.attname, a.attnum " + "from pg_namespace n " + "join pg_class c on c.relnamespace = n.oid "
        + "join pg_index i on i.indrelid = c.oid "
        + "join pg_attribute a on a.attrelid = c.oid and a.attnum = any(i.indkey) "
        + "where n.nspname = ? and c.oid = (n.nspname || '.' || ?)::regclass and i.indisprimary order by a.attnum";
    try (PreparedStatement ps = conn.prepareStatement(sql);) {
      ps.setString(1, schema);
      ps.setString(2, table.getCanonicalName());
      try (ResultSet rs = ps.executeQuery();) {
        List<PKColumn> pkColumns = new ArrayList<>();
        while (rs.next()) {
          String name = readString(rs, 1);
          Integer position = readInt(rs, 2);
          PKColumn c = new PKColumn(position, name);
          pkColumns.add(c);
        }
        return pkColumns;
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
    if (canonicalName.matches("^[a-z0-9_]+$")) {
      return canonicalName;
    } else {
      return "\"" + canonicalName.replace("\"", "\"\"") + "\"";
    }
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
    return limit == null ? "" : ("limit " + limit);
  }

  @Override
  public Boolean getDefaultAutoCommit() {
    return false;
  }

  @Override
  public String renderNullsOrdering(boolean nullsFirst) {
    return nullsFirst ? " nulls first" : " nulls last";
  }

}
