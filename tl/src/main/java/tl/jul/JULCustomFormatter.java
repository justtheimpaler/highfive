package tl.jul;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.StackWalker.StackFrame;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class JULCustomFormatter extends Formatter {

  private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("uuuu-MM-dd hh:mm:ss");

  public static void initialize(Level level) {
    var root = Logger.getLogger("");
    root.setLevel(level);
    JULCustomFormatter f = new JULCustomFormatter();
    for (var handler : root.getHandlers()) {
      handler.setFormatter(f);
      handler.setLevel(level);
    }
  }

  @Override
  public String format(LogRecord record) {
    var builder = new StringBuilder();
    appendDateTime(record, builder);
    appendLevel(record, builder);
    appendClassNameAndLineNumber(record, builder);
    appendMessage(record, builder);
    appendThrown(record, builder);
    builder.append("\n");
    return builder.toString();
  }

  private void appendDateTime(LogRecord record, StringBuilder builder) {
    var zdt = record.getInstant().atZone(ZoneId.systemDefault());
    builder.append(DTF.format(zdt));
  }

  private void appendLevel(LogRecord record, StringBuilder builder) {
    var name = renderLevel(record);
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
    var frame = new SourceFinder().get();
    if (frame != null) {
      var className = frame.getClassName();
      int index = className.lastIndexOf('.');
      if (index != -1) {
        className = className.substring(index + 1);
      }
      builder.append(" ").append(className).append("(").append(frame.getLineNumber()).append(")");
    }
  }

  private void appendMessage(LogRecord record, StringBuilder builder) {
    builder.append(" - ").append(formatMessage(record));
  }

  private void appendThrown(LogRecord record, StringBuilder builder) {
    var thrown = record.getThrown();
    if (thrown != null) {
      var sw = new StringWriter();
      thrown.printStackTrace(new PrintWriter(sw));
      builder.append(System.lineSeparator()).append(sw.toString());
    }
  }

  private static class SourceFinder implements Predicate<StackFrame>, Supplier<StackFrame> {

    private static final StackWalker WALKER = StackWalker.getInstance();

    private boolean foundLogger;

    @Override
    public StackFrame get() {
      return WALKER.walk(s -> s.filter(this).findFirst()).orElse(null);
    }

    @Override
    public boolean test(StackFrame t) {
      if (!foundLogger) {
        foundLogger = t.getClassName().equals(Logger.class.getName());
        return false;
      }
      return !t.getClassName().startsWith("java.util.logging");
    }
  }
}