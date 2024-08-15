package highfive.model;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import highfive.dialects.DB2Dialect;
import highfive.dialects.MariaDBDialect;
import highfive.dialects.MySQLDialect;
import highfive.dialects.OracleDialect;
import highfive.dialects.PostgreSQLDialect;
import highfive.dialects.SQLServerDialect;
import highfive.exceptions.UnsupportedDatabaseTypeException;

public class DialectFactory {

  public static Dialect getDialect(final DataSource ds) throws SQLException, UnsupportedDatabaseTypeException {

    Connection conn = ds.getConnection();

    final DatabaseMetaData dm = conn.getMetaData();

    String name = dm.getDatabaseProductName();
    String version = dm.getDatabaseProductVersion();
    String uName = name.toUpperCase();
    if (uName.startsWith("ORACLE")) {
      return new DialectNormalizer(new OracleDialect(ds, conn));
    } else if (uName.startsWith("DB2/")) {
      return new DialectNormalizer(new DB2Dialect(ds, conn));
    } else if (uName.startsWith("POSTGRESQL")) {
      return new DialectNormalizer(new PostgreSQLDialect(ds, conn));
    } else if (name.startsWith("Microsoft SQL Server")) {
      return new DialectNormalizer(new SQLServerDialect(ds, conn));
    } else if (name.equals("MySQL")) {
      if (version.indexOf("MariaDB") != -1) {
        return new DialectNormalizer(new MariaDBDialect(ds, conn));
      } else {
        return new DialectNormalizer(new MySQLDialect(ds, conn));
      }
    } else {
      throw new UnsupportedDatabaseTypeException("Could not resolve the database dialect. "
          + "The product name reported by the JDBC driver '" + name + "' is not supported by HighFive.");
    }

  }

}
