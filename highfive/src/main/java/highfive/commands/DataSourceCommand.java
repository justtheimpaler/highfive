package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.logging.Logger;

import highfive.BuildInformation;
import highfive.exceptions.CouldNotCopyDataException;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.DataSource;
import highfive.model.Table;
import highfive.model.TableFilter;
import highfive.model.TableHashingOrdering;
import highfive.utils.Name;

public abstract class DataSourceCommand extends Command {

  private static final Logger log = Logger.getLogger(DataSourceCommand.class.getName());

  protected DataSource ds;

  public DataSourceCommand(final String commandName, final String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super(commandName);
    info("HighFive " + BuildInformation.VERSION + " - build " + BuildInformation.BUILD_ID + " - Command: "
        + super.getCommandName());
    info(" ");
    this.ds = DataSource.load(datasourceName);
  }

  @Override
  public void run() throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      NoSuchAlgorithmException, CouldNotHashException, IOException, InvalidHashFileException, CouldNotCopyDataException,
      InvalidConfigurationException {

    this.ds.show("DataSource");

    this.execute();

  }

  public abstract void execute() throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      NoSuchAlgorithmException, CouldNotHashException, IOException, InvalidHashFileException, CouldNotCopyDataException,
      InvalidConfigurationException;

  protected static final DecimalFormat DF = new DecimalFormat("#,##0");

  protected void displayRowCount(List<Table> tables) throws SQLException {
    info(" ");
    info("Row Count:");
    for (Table t : tables) {
      String tid = this.ds.getDialect().renderSQLTableIdentifier(t.getIdentifier());
      String sql = "select count(*) from " + tid;
      try (PreparedStatement ps = this.ds.getConnection().prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
        if (rs.next()) {
          long count = rs.getLong(1);
          info("  " + t.getIdentifier().getGenericName() + ": " + DF.format(count) + " rows");
        }
      }
    }
  }

  protected void checkIfHashingAndCopyingIsSupported(final List<Table> tables)
      throws SQLException, UnsupportedDatabaseTypeException, InvalidConfigurationException {

    info(" ");
    info("Hashing & copying preconditions:");

    int tablesSortable = 0;
    int tableCount = 0;

    int columnsSupported = 0;
    int columnCount = 0;

    for (Name na : this.ds.getTableFilter().listNotAccepted()) {
      error("  - Table " + na + " not found.");
    }

    for (Table t : tables) {
      tableCount++;
      boolean hasPK = false;
      for (Column c : t.getColumns()) {
        columnCount++;
        if (c.getPKPosition() != null) {
          hasPK = true;
        }
        if (c.getSerializer() != null) {
          columnsSupported++;
        } else {
          error("  - Unsupported data type of column " + t.getIdentifier().getCanonicalName() + "."
              + c.getCanonicalName() + ": " + c.getRenderedType());
        }
      }
      TableHashingOrdering tho = this.ds.getHashingOrderings().get(t.getIdentifier().getGenericName());
      if (tho != null) {
        try {
          tho.validate(this.ds, t);
          tablesSortable++;
        } catch (InvalidConfigurationException e) {
          throw e;
        }
      } else if (hasPK) {
        tablesSortable++;
      } else {
        error("  - Table " + t.getIdentifier().getCanonicalName() + " does not have a sorting order for hashing. "
            + "Add a primary key or declare a unique criteria for ordering using the property '" + this.ds.getName()
            + ".hashing.ordering'.");
      }
    }

    TableFilter tf = this.ds.getTableFilter();

    boolean allTablesFound = tf.allTablesFound();
    boolean allTablesSortable = tablesSortable >= tableCount;
    boolean allColumnsSupported = columnsSupported >= columnCount;
    boolean schemaCanBeHashed = allTablesFound && allTablesSortable && allColumnsSupported;
    boolean schemaCanBeCopied = allTablesFound && allColumnsSupported;

    if (this.ds.getTableFilter().declared()) {
      info("  Tables found" + (tf.size() > 0 ? (" (" + tf.found() + "/" + tf.size() + ")") : "") + " - "
          + (allTablesFound ? "PASS" : "FAIL"));
    }
    info("  Tables have primary key and/or have a declared hashing ordering (" + tablesSortable + "/" + tableCount
        + ") - " + (allTablesSortable ? "PASS" : "FAIL"));
    info("  Column types are supported (" + columnsSupported + "/" + columnCount + ") - "
        + (allColumnsSupported ? "PASS" : "FAIL"));
    if (schemaCanBeCopied) {
      info("  The schema is suitable for data copy.");
    } else {
      info("  The schema is not suitable data copy.");
    }
    if (schemaCanBeHashed) {
      info("  The schema data can be hashed.");
    } else {
      info("  The schema data cannot be hashed.");
    }

  }

}
