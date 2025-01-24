package highfive.commands;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.commands.consumer.HashConsumer;
import highfive.commands.consumer.HashConsumer.ExecutionStatus;
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
    super("Hash Dump Compare", datasourceName);
    this.hashDumpConfig = hashDumpConfig;
  }

  @Override
  public void execute()
      throws NoSuchAlgorithmException, SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      CouldNotHashException, IOException, InvalidHashFileException, InvalidConfigurationException {

    info("");

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();
    Identifier tn = findTable(hashDumpConfig.getTableName(), tableNames);
    if (tn == null) {
      throw new CouldNotHashException("Could not find the table '" + hashDumpConfig.getTableName() + "'");
    }
    Table t = this.ds.getDialect().getTableMetaData(tn);

    File f = new File(this.ds.getHashDumpFileName());

    try (HashConsumer hc = hashDumpConfig.getHashConsumer(f)) {
      super.hashOneTable(t, hc);
      ExecutionStatus status = hc.getStatus();
      if (status.successful()) {
        info("Hash dump comparison successful -- The live table '" + hashDumpConfig.getTableName()
            + "' fully matches the baseline dump file.");
      } else {
        error("Hash dump comparison failed -- " + status.getErrorMessage());
      }

    } catch (Exception e) {
      e.printStackTrace(System.out);
      throw new CouldNotHashException(e.getMessage());
    }

  }

}
