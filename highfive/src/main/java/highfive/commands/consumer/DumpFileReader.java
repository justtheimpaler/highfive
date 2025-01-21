package highfive.commands.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

public class DumpFileReader {

// # table: employee (whole table)
// # table: employee (rows 1000-2000)
// # table: employee (rows 1000-2000, step 100)

  private static final Logger log = Logger.getLogger(DumpFileReader.class.getName());

  private File f;
  private BufferedReader r;

  private long lineNo;
  private boolean eof;

  private String hash;
  private long row;

  public DumpFileReader(File f) throws InvalidDumpFileException {
    log.fine("init");
    this.f = f;
    this.lineNo = 0;
    this.eof = false;
    try {
      this.r = new BufferedReader(new FileReader(this.f));
      readHeader();
      next();
    } catch (IOException e) {
      throw new InvalidDumpFileException(e);
    }
  }

  private void readHeader() throws InvalidDumpFileException {
    String txt;
    try {
      txt = this.r.readLine();
    } catch (IOException e) {
      throw new InvalidDumpFileException(e);
    }
    if (txt == null) {
      this.eof = true;
      throw new InvalidDumpFileException(
          "Could not read the dump file '" + this.f + "'; it does not have the first comment line.");
    }
    this.lineNo++;
  }

  private void next() throws InvalidDumpFileException {
    if (this.eof) {
      throw new InvalidDumpFileException(
          "Could not read the baseline file '" + this.f + "'; the end of the file was already reached.");
    } else {
      String txt;
      try {
        txt = this.r.readLine();
      } catch (IOException e) {
        throw new InvalidDumpFileException(e);
      }
      if (txt == null) {
        this.hash = null;
        this.row = -1;
        this.eof = true;
      } else {
        this.lineNo++;
        if (txt.length() < 64 + 1 + 1) {
          this.hash = null;
          this.row = -1;
          throw new InvalidDumpFileException(
              "Could not read the file '" + this.f + "'; Invalid format of line #" + this.lineNo + ": " + txt);
        } else {
          this.hash = txt.substring(0, 64);
          this.row = Long.parseLong(txt.substring(65));
        }
      }
    }
  }

  // Getters

  public String getHash() {
    return hash;
  }

  public long getRow() {
    return row;
  }

  // Exception

  public class InvalidDumpFileException extends Exception {

    private static final long serialVersionUID = 1L;

    public InvalidDumpFileException(String message) {
      super(message);
    }

    public InvalidDumpFileException(Throwable cause) {
      super(cause);
    }

  }

}
