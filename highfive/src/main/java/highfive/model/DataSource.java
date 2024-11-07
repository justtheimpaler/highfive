package highfive.model;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.JavaTypeNotSupportedException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.utils.Name;
import highfive.utils.Utl;

public class DataSource {

  private static final String CONFIG_FILE = "application.properties";

  private static final long DEFAULT_INSERT_BATCH_SIZE = 100L;

  private String name;
  private String driverJAR;
  private String driverClass;
  private String url;
  private String username;
  private String password;
  private String catalog;
  private String schema;
  private String removeTablePrefix;

  private boolean selectAutoCommit;
  private boolean readOnly;
  private TableFilter tableFilter;
  private ColumnFilter columnFilter;
  private TypeSolver solver;
  private Long maxRows;
  private boolean logHashingValues;
  private boolean logSQL;
  private long insertBatchSize;
  private LinkedHashMap<String, TableHashingOrdering> hashingOrderings;

  private String hashFileName;

  private Dialect dialect;
  private Connection conn;

  private String database;
  private String jdbcDriver;

  public DataSource(String name, String driverJAR, String driverClass, String url, String username, String password,
      String catalog, String schema, String removeTablePrefix, final Boolean declaredSelectAutoCommit,
      final boolean readOnly, TableFilter tableFilter, ColumnFilter columnFilter, Long maxRows,
      boolean logHashingValues, boolean logSQL, long insertBatchSize, TypeSolver solver,
      LinkedHashMap<String, TableHashingOrdering> hashingOrderings)
      throws SQLException, UnsupportedDatabaseTypeException {
    this.name = name;
    this.driverJAR = driverJAR;
    this.driverClass = driverClass;
    this.url = url;
    this.username = username;
    this.password = password;
    this.catalog = catalog;
    this.schema = schema;
    this.removeTablePrefix = removeTablePrefix;
    this.readOnly = readOnly;
    this.tableFilter = tableFilter;
    this.columnFilter = columnFilter;
    this.maxRows = maxRows;
    this.logHashingValues = logHashingValues;
    this.logSQL = logSQL;
    this.insertBatchSize = insertBatchSize;
    this.solver = solver;
    this.hashingOrderings = hashingOrderings;

    this.hashFileName = name + ".hash";

    // 1. Load the JDBC driver jar file

    File myJar = new File(this.driverJAR);
    if (!myJar.exists()) {
      error("Could not find the JDBC driver jar library '" + this.driverJAR + "': file not found.");
      throw new SQLException("JDBC driver jar library not found.");
    }

    URL jarURL;
    try {
      jarURL = myJar.toURI().toURL();
    } catch (MalformedURLException e) {
      error("Could not load the JDBC driver jar library: " + e.getMessage());
      throw new SQLException(e.getMessage());
    }

    URLClassLoader child = new URLClassLoader(new URL[] { jarURL }, this.getClass().getClassLoader());

    try {
      Driver dx = (Driver) Class.forName(this.driverClass, true, child).getConstructor().newInstance();
      DriverManager.registerDriver(new DriverShim(dx));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
        | InvocationTargetException | NoSuchMethodException | SecurityException e) {
      error("Could not load the JDBC driver jar library: " + e.getMessage());
      throw new SQLException(e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    // 2. Get a JDBC Connection

    this.conn = null;
    try {
      conn = DriverManager.getConnection(this.url, this.username, this.password);
    } catch (SQLException e) {
      error("Could not connect to database: " + e.getMessage());
      throw e;
    }

    DatabaseMetaData dm = conn.getMetaData();
    this.database = dm.getDatabaseProductName() + " " + dm.getDatabaseProductVersion();
    this.jdbcDriver = dm.getDriverName() + " " + dm.getDriverVersion() + " - implements JDBC "
        + dm.getJDBCMajorVersion() + "." + dm.getJDBCMinorVersion();

    // 3. Discover the Dialect

    this.dialect = DialectFactory.getDialect(this);

    // 4. Decide the autocommit mode

    if (declaredSelectAutoCommit != null) {
      this.selectAutoCommit = declaredSelectAutoCommit;
    } else {
      Boolean defaultAutoCommit = this.dialect.getDefaultAutoCommit();
      if (defaultAutoCommit != null) {
        this.selectAutoCommit = defaultAutoCommit;
      } else {
        this.selectAutoCommit = true;
      }
    }

  }

  public static DataSource load(final String name)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {

    Properties props = new Properties();

    if (name == null || name.trim().isEmpty()) {
      throw new InvalidConfigurationException("Must provide a non-empty db name.");
    }
    if (!name.matches("^[A-Za-z0-9_]+$")) {
      throw new InvalidConfigurationException(
          "The db name can only include letter, digits, and underscores, but found other characters.");
    }

    try {
      props.load(new FileReader(new File(CONFIG_FILE)));
    } catch (IOException e) {
      throw new InvalidConfigurationException(
          "Could not read configuration file '" + CONFIG_FILE + "': " + e.getMessage());
    }
    String driverJAR = readMandatory(props, name + ".driver.jar");
    String driverClass = readMandatory(props, name + ".driver.class");
    String url = readMandatory(props, name + ".url");
    String username = readMandatory(props, name + ".username");
    String password = props.getProperty(name + ".password");
    String catalog = props.getProperty(name + ".catalog");
    String schema = readMandatory(props, name + ".schema");
    String tableFilterList = props.getProperty(name + ".table.filter");
    String removeTablePrefix = props.getProperty(name + ".remove.table.prefix");
    if (Utl.empty(removeTablePrefix)) {
      removeTablePrefix = null;
    } else {
      removeTablePrefix = removeTablePrefix.toLowerCase();
    }
    String columnFilterList = props.getProperty(name + ".column.filter");
    String sMaxRows = props.getProperty(name + ".max.rows");
    String sLogHashingValues = props.getProperty(name + ".log.hashing.values");
    String sLogSQL = props.getProperty(name + ".log.sql");
    String sInsertBatchSize = props.getProperty(name + ".insert.batch.size");
    String typeRules = props.getProperty(name + ".type.rules");

    // Autocommit

    Boolean declaredSelectAutoCommit = null;
    String sDeclaredAutoCommit = props.getProperty(name + ".select.autocommit");
    if (Utl.empty(sDeclaredAutoCommit)) {
      // null
    } else if ("false".equals(sDeclaredAutoCommit)) {
      declaredSelectAutoCommit = false;
    } else if ("true".equals(sDeclaredAutoCommit)) {
      declaredSelectAutoCommit = true;
    } else {
      throw new InvalidConfigurationException("If the property '" + name + ".select.autocommit"
          + "' is specified it must be either 'true' or 'false', but found '" + sDeclaredAutoCommit + "'.");
    }

    // Readonly

    boolean readOnly = true;
    String sReadOnly = props.getProperty(name + ".readonly");
    if (Utl.empty(sReadOnly)) {
      // leave default value
    } else if ("false".equals(sReadOnly)) {
      readOnly = false;
    } else if ("true".equals(sReadOnly)) {
      readOnly = true;
    } else {
      throw new InvalidConfigurationException("If the property '" + name + ".readonly"
          + "' is specified it must be either 'true' or 'false', but found '" + sReadOnly + "'.");
    }

    // Table Filter

    Set<String> allowedTables = new HashSet<>();
    if (tableFilterList != null && !tableFilterList.trim().isEmpty()) {
      String[] parts = tableFilterList.split(",");
      if (parts != null) {
        allowedTables = Arrays.stream(parts).map(t -> Name.lower(t.trim())).collect(Collectors.toSet());
      }
    }
    TableFilter tableFilter = new TableFilter(allowedTables);

    // Column Filter

    Set<String> allowedColumns = new HashSet<>();
    if (columnFilterList != null && !columnFilterList.trim().isEmpty()) {
      String[] parts = columnFilterList.split(",");
      if (parts != null) {
        allowedColumns = Arrays.stream(parts).map(t -> Name.lower(t.trim())).collect(Collectors.toSet());
      }
    }
    ColumnFilter columnFilter = new ColumnFilter(allowedColumns);

    // Max rows

    Long maxRows = null;
    if (sMaxRows != null && !sMaxRows.trim().isEmpty()) {
      try {
        maxRows = Long.parseLong(sMaxRows);
        if (maxRows < 1) {
          throw new InvalidConfigurationException("If the property '" + name + ".max.rows"
              + "' is specified, if must be a number greater than zero, but found '" + sMaxRows + "'.");
        }
      } catch (NumberFormatException e) {
        throw new InvalidConfigurationException("If the property '" + name + ".max.rows"
            + "' is specified, if must be a number greater than zero, but found '" + sMaxRows + "'.");
      }
    }

    // Log Hashing Values

    boolean logHashingValues = false;
    if (Utl.empty(sLogHashingValues)) {
      // leave default value
    } else if ("false".equals(sLogHashingValues)) {
      logHashingValues = false;
    } else if ("true".equals(sLogHashingValues)) {
      logHashingValues = true;
    } else {
      throw new InvalidConfigurationException("If the property '" + name + ".log.hashing.values"
          + "' is specified it must be either 'true' or 'false', but found '" + sReadOnly + "'.");
    }

    // Log SQL

    boolean logSQL = false;
    if (Utl.empty(sLogSQL)) {
      // leave default value
    } else if ("false".equals(sLogSQL)) {
      logSQL = false;
    } else if ("true".equals(sLogSQL)) {
      logSQL = true;
    } else {
      throw new InvalidConfigurationException("If the property '" + name + ".log.sql"
          + "' is specified it must be either 'true' or 'false', but found '" + sReadOnly + "'.");
    }

    // Hashing Ordering

    LinkedHashMap<String, TableHashingOrdering> hashingOrderings = new LinkedHashMap<>();

    String sSortingColumns = props.getProperty(name + ".hashing.ordering");
    if (!Utl.empty(sSortingColumns)) {
      String[] tparts = sSortingColumns.split(";");
      for (String tp : tparts) {
        int idx = tp.indexOf(":");
        if (idx == -1) {
          throw new InvalidConfigurationException("If the property '" + name + ".hashing.ordering"
              + "' is specified, it must be a semicolon-separated list of table sorting rules, "
              + "where each rule takes the form '<table>:<column>,<column>,...'");
        }

        String tname = tp.substring(0, idx).toLowerCase().trim();
        TableHashingOrdering tho = new TableHashingOrdering(tname);
        if (hashingOrderings.containsKey(tname)) {
          throw new InvalidConfigurationException(
              "Duplicate table name '" + tname + "' found in the property '" + name + ".hashing.ordering"
                  + "'. When specified, this property must be a semicolon-separated list of table sorting rules, "
                  + "where each rule takes the form '<table>:<column>,<column>,...'");
        }
        hashingOrderings.put(tho.getTableName(), tho);

        String smembers = tp.substring(idx + 1).trim().toLowerCase();
        if (!smembers.equals("*")) {
          String[] sm = smembers.split(",");
          for (String s : sm) {
            TableHashingMember m = parseOrderingMember(name, tname, s);
            if (tho.getMembers().containsKey(m.getGenericColumnName())) {
              throw new InvalidConfigurationException("Duplicate column name '" + m.getGenericColumnName()
                  + "' found in the property '" + name + ".hashing.ordering" + "' for table '" + tname + "'.");
            }
            tho.getMembers().put(m.getGenericColumnName(), m);
          }
        }

      }
    }

    // Insert Batch Size

    long insertBatchSize = DEFAULT_INSERT_BATCH_SIZE;
    if (!Utl.empty(sInsertBatchSize)) {
      try {
        insertBatchSize = Long.parseLong(sInsertBatchSize);
        if (insertBatchSize < 1) {
          throw new InvalidConfigurationException("If the property '" + name + ".insert.batch.size"
              + "' is specified, if must be a number greater than zero, but found '" + sInsertBatchSize + "'.");
        }
      } catch (NumberFormatException e) {
        throw new InvalidConfigurationException("If the property '" + name + ".insert.batch.size"
            + "' is specified, if must be a number greater than zero, but found '" + sInsertBatchSize + "'.");
      }
    }

    // Type Rules

    TypeSolver solver = new TypeSolver();
    if (!Utl.empty(typeRules)) {
      String[] parts = typeRules.split(";");
      for (String p : parts) {
        int c = p.indexOf(':');
        if (c == -1) {
          throw new InvalidConfigurationException("If the property '" + name + ".type.rules"
              + "' is specified, if must be a semicolon-separated list of rules; each rule takes the form 'dbtype:javatype'. Invalid value: "
              + typeRules);
        }
        String dt = p.substring(0, c);
        String jt = p.substring(c + 1);
        try {
          solver.add(dt, jt);
        } catch (JavaTypeNotSupportedException e) {
          throw new InvalidConfigurationException("Invalid configuration: " + e.getMessage());
        }
      }
    }

    return new DataSource(name, driverJAR, driverClass, url, username, password, catalog, schema, removeTablePrefix,
        declaredSelectAutoCommit, readOnly, tableFilter, columnFilter, maxRows, logHashingValues, logSQL,
        insertBatchSize, solver, hashingOrderings);

  }

  private static TableHashingMember parseOrderingMember(String dsName, String table, String s)
      throws InvalidConfigurationException {
    // t1.hashing.ordering=t:a/desc/nf,b; u:*
    String[] parts = s.split("\\/");
    String p1 = parts[0].trim().toLowerCase();
    if (Utl.empty(p1)) {
      throw new InvalidConfigurationException(
          "Empty column name for table '" + table + "' found in the property '" + dsName + ".hashing.ordering"
              + "'. When specified, this property must be a semicolon-separated list of table sorting rules, "
              + "where each rule takes the form '<table>:<column>,<column>,...'");
    }
    if (parts.length == 1) {
      return new TableHashingMember(p1, true, null);
    } else if (parts.length == 2) {
      String p2 = parts[1].trim().toLowerCase();
      if (p2.equals("desc")) {
        return new TableHashingMember(p1, false, null);
      } else if (p2.equals("nf")) {
        return new TableHashingMember(p1, true, true);
      } else if (p2.equals("nl")) {
        return new TableHashingMember(p1, true, false);
      } else {
        throw new InvalidConfigurationException(
            "Invalid ordering column option '" + p2 + "' for table '" + table + "' in the property '" + dsName
                + ".hashing.ordering" + "'. Valid ordering column options are: desc, nf, nl");
      }
    } else if (parts.length == 3) {
      String p2 = parts[1].trim().toLowerCase();
      if (!p2.equals("desc")) {
        throw new InvalidConfigurationException("Invalid ordering column option '" + p2 + "' for table '" + table
            + "' in the property '" + dsName + ".hashing.ordering"
            + "'. When three ordering column options are specified, the second one must be 'desc'");
      }
      String p3 = parts[2].trim().toLowerCase();
      if (p3.equals("nf")) {
        return new TableHashingMember(p1, false, true);
      } else if (p3.equals("nl")) {
        return new TableHashingMember(p1, false, false);
      } else {
        throw new InvalidConfigurationException("Invalid ordering column option '" + p3 + "' for table '" + table
            + "' in the property '" + dsName + ".hashing.ordering"
            + "'. When three ordering column options are specified, the third one one must be either 'nf' or 'nl'");
      }
    } else {
      throw new InvalidConfigurationException(
          "Invalid ordering for table '" + table + "' in the the property '" + dsName + ".hashing.ordering"
              + "'. When specified, this property must be a semicolon-separated list of table sorting rules, "
              + "where each rule takes the form '<table>:<column>,<column>,...'");
    }
  }

  private static String readMandatory(final Properties props, final String name) throws InvalidConfigurationException {
    String value = props.getProperty(name);
    if (value == null) {
      throw new InvalidConfigurationException("Could not find property '" + name + "'.");
    }
    return value;
  }

  public void show(String datasourceName) {
    this.show(datasourceName, false);
  }

  public void show(String datasourceName, boolean forInserting) {
    info(datasourceName + ":");
    info("  name: " + this.name);
    info("  dialect: " + this.getDialect().getName());
    info("  url: " + this.url);
    info("  database: " + this.database);
    info("  JDBC Driver: " + this.jdbcDriver);
    info("  username: " + this.username);
    info("  catalog: " + (this.catalog == null ? "" : this.catalog));
    info("  schema: " + this.schema);

    // Optional Properties

    if (this.tableFilter.declared()) {
      info("  table filter: " + this.tableFilter.render());
    }
    if (this.removeTablePrefix != null) {
      info("  remove table prefix: " + this.removeTablePrefix);
    }
    if (this.columnFilter.declared()) {
      info("  column filter: " + this.columnFilter.render());
    }
    if (this.maxRows != null) {
      info("  max rows to read: " + this.maxRows);
    }
    if (this.logHashingValues) {
      info("  log hashing values: " + this.logHashingValues);
    }
    if (this.logSQL) {
      info("  log SQL: " + this.logSQL);
    }
    if (forInserting) {
      info("  insert batch size: " + this.insertBatchSize);
    }

  }

  // Getters

  public String getName() {
    return name;
  }

  public String getDriverJAR() {
    return driverJAR;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public String getURL() {
    return url;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getRemoveTablePrefix() {
    return removeTablePrefix;
  }

  public TableFilter getTableFilter() {
    return tableFilter;
  }

  public ColumnFilter getColumnFilter() {
    return columnFilter;
  }

  public Long getMaxRows() {
    return maxRows;
  }

  public boolean getLogHashingValues() {
    return logHashingValues;
  }

  public boolean getLogSQL() {
    return logSQL;
  }

  public long getInsertBatchSize() {
    return insertBatchSize;
  }

  public TypeSolver getTypeSolver() {
    return solver;
  }

  public String getHashFileName() {
    return hashFileName;
  }

  public Dialect getDialect() {
    return dialect;
  }

  public boolean getSelectAutoCommit() {
    return selectAutoCommit;
  }

  public boolean getReadOnly() {
    return readOnly;
  }

  public LinkedHashMap<String, TableHashingOrdering> getHashingOrderings() {
    return hashingOrderings;
  }

  public String getDatabase() {
    return database;
  }

  public String getJDBCDriver() {
    return jdbcDriver;
  }

  // Utils

  public Connection getConnection() {
    return conn;
  }

  private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  protected static void info(final String s) {
    System.out.println(DF.format(new Date()) + " INFO  - " + s);
  }

  protected void error(final String s) {
    System.out.println(DF.format(new Date()) + " ERROR - " + s);
  }

  protected void error(final Throwable e) {
    System.out.print(DF.format(new Date()) + " ERROR - ");
    e.printStackTrace(System.out);
  }

}
