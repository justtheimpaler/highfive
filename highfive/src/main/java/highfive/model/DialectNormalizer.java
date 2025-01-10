package highfive.model;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.exceptions.UnsupportedSQLFeatureException;

public class DialectNormalizer extends Dialect {

  private Dialect dialect;

  public DialectNormalizer(Dialect dialect) {
    super(dialect.ds, dialect.conn);
    this.dialect = dialect;
  }

  @Override
  public String getName() {
    return this.dialect.getName();
  }

  @Override
  public List<Identifier> listTablesNames() throws SQLException, InvalidSchemaException {
    List<Identifier> tables = this.dialect.listTablesNames();
    Collections.sort(tables, (a, b) -> a.getGenericName().compareTo(b.getGenericName()));
    return tables;
  }

  @Override
  public Table getTableMetaData(Identifier table) throws SQLException, UnsupportedDatabaseTypeException {
    Table t = this.dialect.getTableMetaData(table);
    t.sortColumns();
    return t;
  }

  @Override
  public String escapeIdentifierAsNeeded(String identifier) {
    return this.dialect.escapeIdentifierAsNeeded(identifier);
  }

  @Override
  public String addCollation(String columnCanonicalName, String collation) {
    return this.dialect.addCollation(columnCanonicalName, collation);
  }

  @Override
  public String renderSQLTableIdentifier(Identifier table) {
    return this.dialect.renderSQLTableIdentifier(table);
  }

  @Override
  public String renderHeadLimit(Long limit) {
    return this.dialect.renderHeadLimit(limit);
  }

  @Override
  public String renderTailLimit(Long limit) {
    return this.dialect.renderTailLimit(limit);
  }

  @Override
  public Boolean getDefaultAutoCommit() {
    return this.dialect.getDefaultAutoCommit();
  }

  @Override
  protected Serializer<?> getDefaultSerializer(Identifier table, String name, String type, boolean unsigned,
      BigInteger maxLength, Integer precision, Integer scale) throws UnsupportedDatabaseTypeException {
    return this.dialect.getDefaultSerializer(table, name, type, unsigned, maxLength, precision, scale);
  }

  @Override
  public String renderNullsOrdering(boolean nullsFirst) throws UnsupportedSQLFeatureException {
    return this.dialect.renderNullsOrdering(nullsFirst);
  }

}
