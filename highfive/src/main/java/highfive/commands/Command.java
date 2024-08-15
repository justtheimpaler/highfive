package highfive.commands;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import highfive.exceptions.CouldNotCopyDataException;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;

public abstract class Command {

  private String commandName;

  public Command(String commandName) {
    this.commandName = commandName;
  }

  protected String getCommandName() {
    return this.commandName;
  }

  public abstract void run()
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException,
      NoSuchAlgorithmException, CouldNotHashException, IOException, InvalidHashFileException, CouldNotCopyDataException;

  // Utils

  private final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  protected void info(final String s) {
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
