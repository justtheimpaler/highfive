package highfive.commands;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.HashFile;

public class VerifyCommand extends GenericHashCommand {

  private String baselineFile;

  public VerifyCommand(final String datasourceName, final String baselineFile)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("Verify", datasourceName);
    this.baselineFile = baselineFile;
  }

  @Override
  public void execute()
      throws FileNotFoundException, IOException, InvalidHashFileException, NoSuchAlgorithmException, SQLException,
      UnsupportedDatabaseTypeException, InvalidSchemaException, CouldNotHashException, InvalidConfigurationException {

    HashFile existing = HashFile.loadFrom(this.baselineFile);

    super.hash();

    info(" ");
    info("Verifying hashes:");

    List<String> errors = existing.compareTo(hashFile, "baseline hash file", "live database");
    for (String err : errors) {
      error("  - " + err);
    }
    if (errors.isEmpty()) {
      info("  All data hashes match -- The verification succeeded.");
    } else {
      error("  There were a total of " + errors.size() + " difference(s) in the hashes -- The verification failed.");
    }

  }

}
