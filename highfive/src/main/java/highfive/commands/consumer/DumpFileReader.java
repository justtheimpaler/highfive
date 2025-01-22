package highfive.commands.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DumpFileReader {

  private static final Logger log = Logger.getLogger(DumpFileReader.class.getName());

  private File f;
  private BufferedReader r;

  private DumpFileMetaData metadata;
  private long lineNo;
  private boolean eof;
  private boolean error;

  private String hash;
  private long row;

  public DumpFileReader(File f) throws InvalidDumpFileException, DumpFileIOException {
    log.fine("init");
    this.f = f;
    this.lineNo = 0;
    this.eof = false;
    this.error = false;
    try {
      this.r = new BufferedReader(new FileReader(this.f));
      this.metadata = readHeader();
    } catch (IOException e) {
      throw new DumpFileIOException(e);
    }
  }

  private DumpFileMetaData readHeader() throws InvalidDumpFileException, DumpFileIOException {
    String txt;
    try {
      txt = this.r.readLine();
    } catch (IOException e) {
      throw new DumpFileIOException(e);
    }
    if (txt == null) {
      this.eof = true;
      throw new InvalidDumpFileException(
          "Could not read the dump file '" + this.f + "'; it does not have the first comment line.");
    }
    this.lineNo++;
    return DumpFileMetaData.parseHeader(txt);
  }

  public boolean next() throws InvalidDumpFileException, DumpFileIOException {
    if (this.eof) {
      throw new InvalidDumpFileException(
          "Could not read the baseline file '" + this.f + "'; the end of the file was already reached.");
    } else if (this.error) {
      throw new InvalidDumpFileException(
          "Could not read the baseline file '" + this.f + "'; an error had been previously detected.");
    } else {
      String txt;
      try {
        txt = this.r.readLine();
      } catch (IOException e) {
        throw new DumpFileIOException(e);
      }
      if (txt == null) {
        this.hash = null;
        this.row = -1;
        this.eof = true;
        return false;
      } else {
        this.lineNo++;
        if (txt.length() < 64 + 1 + 1) {
          this.hash = null;
          this.row = -1;
          this.error = true;
          throw new InvalidDumpFileException(
              "Could not read the file '" + this.f + "'; Invalid format of line #" + this.lineNo + ": " + txt);
        } else {
          this.hash = txt.substring(0, 64);
          this.row = Long.parseLong(txt.substring(65));
          return true;
        }
      }
    }
  }

  public static enum DumpFileType {
    FULL, RANGED, STEPPED
  };

  public static class DumpFileMetaData {

    private DumpFileType type;
    private String table;
    private Long start;
    private Long end;
    private Long step;

    public DumpFileMetaData(String table) {
      this.type = DumpFileType.FULL;
      this.table = table;
      this.start = null;
      this.end = null;
      this.step = null;
    }

    public DumpFileMetaData(String table, long start, long end) {
      this.type = DumpFileType.RANGED;
      this.table = table;
      this.start = start;
      this.end = end;
      this.step = null;
    }

    public DumpFileMetaData(String table, long start, long end, long step) {
      this.type = DumpFileType.STEPPED;
      this.table = table;
      this.start = start;
      this.end = end;
      this.step = step;
    }

    public DumpFileType getType() {
      return type;
    }

    public String getTable() {
      return table;
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

    public static DumpFileMetaData parseHeader(final String header) throws InvalidDumpFileException {
      DumpFileMetaData m;
      m = isWholeTable(header);
      if (m != null) {
        return m;
      }
      m = isRangedTable(header);
      if (m != null) {
        return m;
      }
      m = isSteppedTable(header);
      if (m != null) {
        return m;
      }
      throw new InvalidDumpFileException("Could not parse the header of the dump file into any valid format.");
    }

    // # table: employee (whole table)
    // # table: employee (rows 1000-2000)
    // # table: employee (rows 1000-2000, step 100)

    private static final Pattern WHOLE_TABLE = Pattern.compile("^# table: (.*) \\(whole table\\)$");

    public static DumpFileMetaData isWholeTable(final String header) {
      Matcher m = WHOLE_TABLE.matcher(header);
      if (!m.matches()) {
        return null;
      } else {
        String table = m.group(1);
        return new DumpFileMetaData(table);
      }
    }

//    System.out.println(m.group(0)); // whole matched expression
//    System.out.println(m.group(1)); // first expression from round brackets (Testing)
//    System.out.println(m.group(2)); // second one (123)
//    System.out.println(m.group(3)); // third one (Testing)  

    private static final Pattern RANGED_TABLE = Pattern.compile("^# table: (.*) \\(rows (\\d+)\\-(\\d+)\\)$");

    public static DumpFileMetaData isRangedTable(final String header) {
//      log.info("R0: '" + header + "'");
      Matcher m = RANGED_TABLE.matcher(header);
      if (!m.matches()) {
//        log.info("R1");
        return null;
      } else {
//        log.info("R2");
        String table = m.group(1);
        Long start = Long.parseLong(m.group(2));
        Long end = Long.parseLong(m.group(3));
        return new DumpFileMetaData(table, start, end);
      }
    }

    private static final Pattern STEPPED_TABLE = Pattern
        .compile("^# table: (.*) \\(rows (\\d+)\\-(\\d+), step (\\d+)\\)$");

    public static DumpFileMetaData isSteppedTable(final String header) {
      Matcher m = STEPPED_TABLE.matcher(header);
      if (!m.matches()) {
        return null;
      } else {
        String table = m.group(1);
        Long start = Long.parseLong(m.group(2));
        Long end = Long.parseLong(m.group(3));
        Long step = Long.parseLong(m.group(4));
        return new DumpFileMetaData(table, start, end, step);
      }
    }

    @Override
    public String toString() {
      return "DumpFileMetaData [type=" + type + ", table=" + table + ", start=" + start + ", end=" + end + ", step="
          + step + "]";
    }

  }

  // Getters

  public DumpFileMetaData getMetadata() {
    return metadata;
  }

  public long getRow() {
    return row;
  }

  public String getHash() {
    return hash;
  }

  // Exception

  public static class InvalidDumpFileException extends Exception {

    private static final long serialVersionUID = 1L;

    public InvalidDumpFileException(String message) {
      super(message);
    }

  }

  public static class DumpFileIOException extends Exception {

    private static final long serialVersionUID = 1L;

    public DumpFileIOException(Throwable cause) {
      super(cause);
    }

  }

}
