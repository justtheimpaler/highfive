package highfive.commands;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Identifier;
import highfive.model.Table;

public class HashCompareCommand extends GenericHashCommand {

  private HashDumpConfig hashDumpConfig;

  public HashCompareCommand(final String datasourceName, final HashDumpConfig hashDumpConfig)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("Hash Compare", datasourceName);
    this.hashDumpConfig = hashDumpConfig;
  }

  @Override
  public void execute()
      throws NoSuchAlgorithmException, SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      CouldNotHashException, IOException, InvalidHashFileException, InvalidConfigurationException {

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();
    Identifier tn = findTable(hashDumpConfig.getTableName(), tableNames);
    if (tn == null) {
      throw new CouldNotHashException("Could not find the table '" + hashDumpConfig.getTableName() + "'");
    }
    Table t = this.ds.getDialect().getTableMetaData(tn);

    File f = new File(this.ds.getHashDumpFileName());

    try (HashConsumer hc = hashDumpConfig.getHashConsumer(f)) {
      info("-- CONSUMER: " + hc);
      super.hashOneTable(t, hc);
    } catch (Exception e) {
      e.printStackTrace(System.out);
      throw new CouldNotHashException(e.getMessage());
    }

  }

}
