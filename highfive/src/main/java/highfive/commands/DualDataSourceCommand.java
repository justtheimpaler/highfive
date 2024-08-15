package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import highfive.exceptions.CouldNotCopyDataException;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.DataSource;

public abstract class DualDataSourceCommand extends DataSourceCommand {

  protected DataSource ds2;

  public DualDataSourceCommand(final String commandName, final String datasourceName1, final String datasourceName2)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super(commandName, datasourceName1);
    this.ds2 = DataSource.load(datasourceName2);
  }

  @Override
  public final void run() throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      NoSuchAlgorithmException, CouldNotHashException, IOException, InvalidHashFileException, CouldNotCopyDataException,
      InvalidConfigurationException {

    this.ds.show("Source Datasource");
    info(" ");

    this.ds2.show("Destination Datasource", true);
    info(" ");

    this.execute();

  }

}
