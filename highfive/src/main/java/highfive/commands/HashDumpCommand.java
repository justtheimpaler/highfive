package highfive.commands;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import highfive.commands.HashConsumer.FullHashDumpWriterFactory;
import highfive.commands.HashConsumer.HashDumpComparatorFactory;
import highfive.commands.HashConsumer.HashDumpWriterFactory;
import highfive.commands.HashConsumer.RangeHashDumpWriterFactory;
import highfive.commands.HashConsumer.SteppedHashDumpWriterFactory;
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
      info("-- CONSUMER: " + hc);
      super.hashOneTable(t, hc);
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

    public static HashDumpConfig forCompare(String tableName, String dumpFile) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      return new HashDumpConfig(tableName, null, null, null, new HashDumpComparatorFactory());
    }

    public static HashDumpConfig of(String tableName, String start, String end) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      long s = parseStart(start);
      long e = parseEnd(start, end, s);
      return new HashDumpConfig(tableName, s, e, null, new RangeHashDumpWriterFactory());
    }

    public static HashDumpConfig of(String tableName, String start, String end, String step) {
      if (Utl.empty(tableName)) {
        throw new RuntimeException("The hashdump command requires a non-empty table name.");
      }
      long s = parseStart(start);
      long e = parseEnd(start, end, s);
      long st = parseStep(start, step);
      return new HashDumpConfig(tableName, s, e, st, new SteppedHashDumpWriterFactory());
    }

    // parsing

    private static long parseEnd(String start, String end, long s) {
      long e;
      try {
        e = Long.parseLong(end);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The hashdump <end> parameter must be a positive integer but found '" + start + "'.");
      }
      if (e < 0) {
        throw new RuntimeException(
            "The hashdump <end> parameter must be a positive integer but found '" + start + "'.");
      }
      if (e < s) {
        throw new RuntimeException(
            "The hashdump <end> parameter must have a value equal or greater than the <start> parameter.");
      }
      return e;
    }

    private static long parseStart(String start) {
      long s;
      try {
        s = Long.parseLong(start);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The hashdump <start> parameter must be a positive integer but found '" + start + "'.");
      }
      if (s < 0) {
        throw new RuntimeException(
            "The hashdump <start> parameter must be a positive integer but found '" + start + "'.");
      }
      return s;
    }

    private static long parseStep(String start, String step) {
      long st;
      try {
        st = Long.parseLong(step);
      } catch (NumberFormatException e1) {
        throw new RuntimeException(
            "The hashdump <step> parameter must be a positive integer but found '" + start + "'.");
      }
      if (st < 1) {
        throw new RuntimeException(
            "The hashdump <step> parameter must be a positive integer greater than zero, but found '" + start + "'.");
      }
      return st;
    }

    public HashConsumer getHashConsumer(File f) throws IOException {
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
