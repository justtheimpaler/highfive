package highfive.commands;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.commands.consumer.HashConsumer;
import highfive.commands.consumer.HashConsumer.ExecutionStatus;
import highfive.commands.consumer.HashDumpWriterFactory;
import highfive.commands.consumer.HashDumpWriterFactory.FullHashDumpWriterFactory;
import highfive.commands.consumer.HashDumpWriterFactory.HashDumpComparatorFactory;
import highfive.commands.consumer.HashDumpWriterFactory.HashDumpLoggerFactory;
import highfive.commands.consumer.HashDumpWriterFactory.RangeHashDumpWriterFactory;
import highfive.commands.consumer.HashDumpWriterFactory.SteppedHashDumpWriterFactory;
import highfive.exceptions.CouldNotHashException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidHashFileException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Identifier;
import highfive.model.Table;
import highfive.utils.Utl;

public class HashDumpCommand extends GenericHashCommand {

  private HashDumpConfig hashDumpConfig;

  public HashDumpCommand(final String datasourceName, final HashDumpConfig hashDumpConfig)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("Hashdump", datasourceName);
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
      super.hashOneTable(t, hc);
      ExecutionStatus status = hc.getStatus();
      if (!status.successful()) {
        error(status.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
      throw new CouldNotHashException(e.getMessage());
    }

  }

  public static class HashDumpConfig {

    private String tableName;
    private Long start;
    private Long end;
    private Long step;
    private HashDumpWriterFactory factory;

    private HashDumpConfig(String tableName, Long start, Long end, Long step, HashDumpWriterFactory factory) {
      this.tableName = tableName;
      this.start = start;
      this.end = end;
      this.step = step;
      this.factory = factory;
    }

    public static HashDumpConfig of(String tableName) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      return new HashDumpConfig(tableName, null, null, null, new FullHashDumpWriterFactory());
    }

    public static HashDumpConfig forCompare(String tableName, String baseline) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      return new HashDumpConfig(tableName, null, null, null, new HashDumpComparatorFactory(new File(baseline)));
    }

    public static HashDumpConfig forLog(String tableName, String start, String end) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashlog command requires a non-empty table name.");
      }
      long s = parseStart("hashlog", start);
      long e = parseEnd("hashlog", end, s);
      return new HashDumpConfig(tableName, null, null, null, new HashDumpLoggerFactory(s, e));
    }

    public static HashDumpConfig of(String tableName, String start, String end) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      long s = parseStart("hashdump", start);
      long e = parseEnd("hashdump", end, s);
      return new HashDumpConfig(tableName, s, e, null, new RangeHashDumpWriterFactory());
    }

    public static HashDumpConfig of(String tableName, String start, String end, String step) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      long s = parseStart("hashdump", start);
      long e = parseEnd("hashdump", end, s);
      long st = parseStep("hashdump", step);
      return new HashDumpConfig(tableName, s, e, st, new SteppedHashDumpWriterFactory());
    }

    // parsing

    private static long parseEnd(String command, String end, long s) {
      long e;
      try {
        e = Long.parseLong(end);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The " + command + " <end> parameter must be a positive integer but found '" + end + "'.");
      }
      if (e < 0) {
        throw new RuntimeException(
            "The " + command + " <end> parameter must be a positive integer but found '" + end + "'.");
      }
      if (e < s) {
        throw new RuntimeException(
            "The " + command + " <end> parameter must have a value equal or greater than the <start> parameter.");
      }
      return e;
    }

    private static long parseStart(String command, String start) {
      long s;
      try {
        s = Long.parseLong(start);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The " + command + " <start> parameter must be a positive integer but found '" + start + "'.");
      }
      if (s < 0) {
        throw new RuntimeException(
            "The " + command + " <start> parameter must be a positive integer but found '" + start + "'.");
      }
      return s;
    }

    private static long parseStep(String command, String step) {
      long st;
      try {
        st = Long.parseLong(step);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The " + command + " <step> parameter must be a positive integer but found '" + step + "'.");
      }
      if (st < 1) {
        throw new RuntimeException("The " + command
            + " <step> parameter must be a positive integer greater than zero, but found '" + step + "'.");
      }
      return st;
    }

    public HashConsumer getHashConsumer(File f) throws IOException, InvalidDumpFileException, DumpFileIOException {
      return this.factory.getInstance(this, f);
    }

    // Getters

    public String getTableName() {
      return tableName;
    }

    public Long getStart() {
      return start;
    }

    public Long getEnd() {
      return end;
    }

    public Long getStep() {
      return step;
    }

  }

}
