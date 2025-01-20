package highfive;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

import highfive.commands.Command;
import highfive.commands.CopyCommand;
import highfive.commands.HashCommand;
import highfive.commands.HashCompareCommand;
import highfive.commands.HashDumpCommand;
import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.commands.HashDupesCommand;
import highfive.commands.ListColumnsAndCheckCommand;
import highfive.commands.ListTablesAndCheckCommand;
import highfive.commands.VerifyCommand;
import highfive.exceptions.ApplicationException;
import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.utils.JULCustomFormatter;

public class HighFive {

  static {
    JULCustomFormatter.initialize(Level.INFO);
  }

  public static void main(final String[] args) {

    try {
      processCommand(args);
    } catch (Throwable e) {
      error(e);
      System.exit(1);
    }
  }

  private static void processCommand(final String[] args) {
    try {
      if (args.length == 2 && "listtables".equals(args[0])) {
        Command c = new ListTablesAndCheckCommand(args[1]);
        try {
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error(e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 2 && "listcolumns".equals(args[0])) {
        Command c = new ListColumnsAndCheckCommand(args[1]);
        try {
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error(e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 2 && "hash".equals(args[0])) {
        try {
          Command c = new HashCommand(args[1]);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not hash data: " + e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 2 && "hashdupes".equals(args[0])) {
        try {
          Command c = new HashDupesCommand(args[1]);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not search for hash dupes: " + e.getMessage());
          System.exit(1);
        }

//    0     1    2        3       4      5
// -- hashd <ds> <table> [<start> <end> [<step>]]
      } else if (args.length == 3 && "hashd".equals(args[0])) {
        System.out.println("D1");
        try {
          HashDumpConfig config = HashDumpConfig.of(args[2]);
          Command c = new HashDumpCommand(args[1], config);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not generate the hashdump: " + e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 5 && "hashd".equals(args[0])) {
        System.out.println("D2");
        try {
          HashDumpConfig config = HashDumpConfig.of(args[2], args[3], args[4]);
          Command c = new HashDumpCommand(args[1], config);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not generate the hashdump: " + e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 6 && "hashd".equals(args[0])) {
        System.out.println("D3");
        try {
          HashDumpConfig config = HashDumpConfig.of(args[2], args[3], args[4], args[5]);
          Command c = new HashDumpCommand(args[1], config);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not generate the hashdump: " + e.getMessage());
          System.exit(1);
        }

        // -- 0---- 1--- 2------ 3
        // -- hashc <ds> <table> <baseline-file>
      } else if (args.length == 4 && "hashc".equals(args[0])) {
        try {
          HashDumpConfig config = HashDumpConfig.forCompare(args[2], args[3]);
          Command c = new HashCompareCommand(args[1], config);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not generate the hashdump: " + e.getMessage());
          System.exit(1);
        }

      } else if (args.length == 3 && "verify".equals(args[0])) {
        try {
          Command c = new VerifyCommand(args[1], args[2]);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not verify data: " + e.getMessage());
          System.exit(1);
        }
      } else if (args.length == 3 && "copy".equals(args[0])) {
        try {
          Command c = new CopyCommand(args[1], args[2]);
          c.run();
          System.exit(0);
        } catch (ApplicationException e) {
          error("Could not copy data: " + e.getMessage());
          System.exit(1);
        }
      } else {
        if (args.length == 0) {
          error("Must specify a command.");
        } else {
          error("Invalid command: " + args[0]);
        }
        info("Usage: java -jar highfive.jar [ listtables <datasource> | listcolumns <datasource> "
            + "| hash <datasource> | verify <datasource> <baseline-file> | copy <from-datasource> <to-datasource> ]");
        System.exit(1);
      }
    } catch (NoSuchAlgorithmException | SQLException | IOException | InvalidConfigurationException
        | UnsupportedDatabaseTypeException e) {
      error(e);
      System.exit(1);
    } catch (RuntimeException e) {
      error(e);
      System.exit(1);
    }
  }

  private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private static void info(final String s) {
    System.out.println(DF.format(new Date()) + " INFO  - " + s);
  }

  private static void error(final String s) {
    System.out.println(DF.format(new Date()) + " ERROR - " + s);
  }

  private static void error(final Throwable e) {
    System.out.print(DF.format(new Date()) + " ERROR - ");
    e.printStackTrace(System.out);
  }

}
