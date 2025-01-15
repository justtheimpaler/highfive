package highfive.dialects;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.DataSource;
import highfive.model.Dialect;
import highfive.model.Identifier;
import highfive.model.Serializer;
import highfive.model.Table;
import highfive.serializers.BigDecimalSerializer;
import highfive.serializers.ByteArraySerializer;
import highfive.serializers.DB2LocalDateSerializer;
import highfive.serializers.DB2LocalDateTimeSerializer;
import highfive.serializers.DB2LocalTimeSerializer;
import highfive.serializers.DoubleSerializer;
import highfive.serializers.IntegerSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.StringSerializer;

public class DB2Dialect extends Dialect {

  public DB2Dialect(DataSource ds, Connection conn) {
    super(ds, conn);
  }

  @Override
  public String getName() {
    return "DB2 LUW";
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = new ArrayList<>();
    String sql = "select name from sysibm.systables where creator = ? and type = 'T'";
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
    String sql = "select name, typename, length, scale, keyseq "
        + "from sysibm.syscolumns where tbcreator = ? and tbname = ?";
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
            Integer precision = readInt(rs, col++);
            BigInteger length = precision == null ? null : BigInteger.valueOf(precision);
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
    if ("CHARACTER".equals(type) || "VARCHAR".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("CLOB".equals(type) || "DBCLOB".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("GRAPHIC".equals(type) || "VARGRAPHIC".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("LONG VARCHAR".equals(type) || "LONG VARGRAPHIC".equals(type)) {
      return type;
    } else if ("SMALLINT".equals(type) || "INTEGER".equals(type) || "BIGINT".equals(type)) {
      return type;
    } else if ("DECIMAL".equals(type)) {
      return type + "(" + precision + ", " + scale + ")";
    } else if ("DECFLOAT".equals(type) || "REAL".equals(type) || "DOUBLE".equals(type)) {
      return type;
    } else if ("DATE".equals(type) || "TIME".equals(type) || "TIMESTAMP".equals(type)) {
      return type;
    } else if ("BLOB".equals(type)) {
      return type + "(" + maxLength + ")";
    } else if ("XML".equals(type)) {
      return type;
    } else {
      return type;
    }
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    if ("CHARACTER".equals(type) || "VARCHAR".equals(type)) {
      return new StringSerializer();
    } else if ("CLOB".equals(type) || "DBCLOB".equals(type)) {
      return new StringSerializer();
    } else if ("GRAPHIC".equals(type) || "VARGRAPHIC".equals(type)) {
      return new StringSerializer();
    } else if ("LONG VARCHAR".equals(type) || "LONG VARGRAPHIC".equals(type)) {
      // These types are obsolete for a long time -- don't even try to read them
      throw new UnsupportedDatabaseTypeException(
          "Unsupported column type for column " + name + " in table " + table + ": " + type);
    } else if ("SMALLINT".equals(type) || "INTEGER".equals(type)) {
      return new IntegerSerializer();
    } else if ("BIGINT".equals(type)) {
      return new LongSerializer();
    } else if ("DECIMAL".equals(type)) {
      return new BigDecimalSerializer();
    } else if ("DECFLOAT".equals(type) || "REAL".equals(type) || "DOUBLE".equals(type)) {
      return new DoubleSerializer();
    } else if ("DATE".equals(type)) {
      return new DB2LocalDateSerializer();
    } else if ("TIME".equals(type)) {
      return new DB2LocalTimeSerializer();
    } else if ("TIMESTAMP".equals(type)) {
      return new DB2LocalDateTimeSerializer();
    } else if ("BLOB".equals(type)) {
      return new ByteArraySerializer();
    } else if ("XML".equals(type)) {
      throw new UnsupportedDatabaseTypeException(
          "Unsupported column type for column " + name + " in table " + table + ": " + type);
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
    return "COLLATION_KEY_BIT(" + columnCanonicalName + ", '" + collation + "')";
  }

  @Override
  public String renderSQLTableIdentifier(Identifier table) {
    return (this.ds.getSchema() == null ? "" : escapeIdentifierAsNeeded(this.ds.getSchema()) + ".") + table.renderSQL();
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
