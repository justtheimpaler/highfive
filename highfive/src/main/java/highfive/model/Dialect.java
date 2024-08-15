package highfive.model;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.exceptions.UnsupportedSQLFeatureException;

public abstract class Dialect {

  protected DataSource ds;
  protected Connection conn;

  public Dialect(DataSource ds, Connection conn) {
    this.ds = ds;
    this.conn = conn;
  }

  protected Serializer<?> getSerializer(String renderedType, Identifier table, String name, String type,
      boolean unsigned, BigInteger maxLength, Integer precision, Integer scale)
      throws UnsupportedDatabaseTypeException {
    Serializer<?> ruleSerializer = ds.getTypeSolver().resolve(renderedType);
    if (ruleSerializer != null) {
      return ruleSerializer;
    }
    return getDefaultSerializer(table, name, type, unsigned, maxLength, precision, scale);
  }

  public abstract String getName();

  public abstract List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException;

  public abstract Table getTableMetaData(Identifier table) throws SQLException, UnsupportedDatabaseTypeException;

  protected abstract Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException;

  public abstract String escapeIdentifierAsNeeded(String canonicalName);

  public abstract String renderSQLTableIdentifier(Identifier table);

  public abstract String renderHeadLimit(Long limit);

  public abstract String renderTailLimit(Long limit);

  public abstract Boolean getDefaultAutoCommit();

  public abstract String renderNullsOrdering(boolean nullsFirst) throws UnsupportedSQLFeatureException;

}
