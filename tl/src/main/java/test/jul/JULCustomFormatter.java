package test.jul;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import test.model.Processor;

public class JULCustomFormatter extends Formatter {

  private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public static void initialize() {

    try {
      LogManager.getLogManager().readConfiguration(new FileInputStream("application.properties"));
      System.out.println(">> config file found.");
    } catch (FileNotFoundException e) {
      // Ignore
      System.out.println(">> config file not found.");
    } catch (SecurityException | IOException e1) {
      e1.printStackTrace();
    }

    Logger root = Logger.getLogger("");
    System.out.println(">> root.getLevel()=" + root.getLevel());
//    root.setLevel(level);
    JULCustomFormatter f = new JULCustomFormatter();
    for (Handler handler : root.getHandlers()) {
      handler.setFormatter(f);
      System.out.println(">> handler.getLevel()=" + handler.getLevel());
//      handler.setLevel(level);
    }
  }

  public static void setLevels() {
    LogManager lm = LogManager.getLogManager();
    String name = Processor.class.getName();
//    System.out.println(">>> name=" + name);
//    Logger l1 = lm.getLogger(name);
//    l1.setLevel(Level.FINER);
  }

  @Override
  public String format(LogRecord record) {
    StringBuilder builder = new StringBuilder();
    appendDateTime(record, builder);
    appendLevel(record, builder);
    appendClassNameAndLineNumber(record, builder);
    appendMessage(record, builder);
    appendThrown(record, builder);
    builder.append("\n");
    return builder.toString();
  }

  private void appendDateTime(LogRecord record, StringBuilder builder) {
    builder.append(DF.format(record.getMillis()));
  }

  private void appendLevel(LogRecord record, StringBuilder builder) {
    String name = renderLevel(record);
    builder.append(" ").append(name);
  }

  private String renderLevel(LogRecord record) {
    int l = record.getLevel().intValue();
    if (l == Level.SEVERE.intValue()) {
      return "SEVER";
    } else if (l == Level.WARNING.intValue()) {
      return "WARN ";
    } else if (l == Level.INFO.intValue()) {
      return "INFO ";
    } else if (l == Level.CONFIG.intValue()) {
      return "CONF ";
    } else if (l == Level.FINE.intValue()) {
      return "FINE ";
    } else if (l == Level.FINER.intValue()) {
      return "FINER";
    } else {
      return "FINST";
    }
  }

  private void appendClassNameAndLineNumber(LogRecord record, StringBuilder builder) {
    StackTraceElement caller = findCaller(Thread.currentThread().getStackTrace());
    if (caller != null) {
      String className = caller.getClassName();
      int index = className.lastIndexOf('.');
      if (index != -1) {
        className = className.substring(index + 1);
      }
      builder.append(" ").append(className).append("(").append(caller.getLineNumber()).append(")");
    }
  }

  private StackTraceElement findCaller(StackTraceElement[] elems) {
    for (StackTraceElement e : elems) {
      boolean internal = e.getClassName() != null && (Thread.class.getName().equals(e.getClassName())
          || this.getClass().getName().equals(e.getClassName()) || e.getClassName().startsWith("java.util.logging."));
      if (!internal) {
        return e;
      }
    }
    return null;
  }

  private void appendMessage(LogRecord record, StringBuilder builder) {
    builder.append(" - ").append(formatMessage(record));
  }

  private void appendThrown(LogRecord record, StringBuilder builder) {
    Throwable thrown = record.getThrown();
    if (thrown != null) {
      StringWriter sw = new StringWriter();
      thrown.printStackTrace(new PrintWriter(sw));
      builder.append(System.lineSeparator()).append(sw.toString());
    }
  }

}