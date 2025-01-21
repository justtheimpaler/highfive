package highfive.commands.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Hasher;

public class HashComparator implements HashConsumer {

  private static final Logger log = Logger.getLogger(HashComparator.class.getName());

  private File baseline;
  private BufferedReader r;

  private long lineNo;
  private boolean eof;

  private String hash;
  private long baselineRow;

  private ExecutionStatus status;

  public HashComparator(String tableName, File baseline, File current) throws IOException {
    log.fine("init");
    this.baseline = baseline;
    this.lineNo = 0;
    this.eof = false;
    this.status = null;
    this.r = new BufferedReader(new FileReader(baseline));
    readInitialComment(); // skip the first line since it's a comment
    nextBaseline();
  }
  
  

  private void readInitialComment() throws IOException {
    String txt = this.r.readLine();
    if (txt == null) {
      this.eof = true;
      this.status = ExecutionStatus.failure(
          "Could not read the baseline file '" + this.baseline + "'; it does not have the first comment line.");
    }
    this.lineNo++;
  }

  private void nextBaseline() throws IOException {
    if (this.status != null) {
      return;
    }
    if (this.eof) {
      this.status = ExecutionStatus
          .failure("Could not read the baseline file '" + this.baseline + "'; end of file already reached.");
    } else {
      String txt = this.r.readLine();
      if (txt == null) {
        this.hash = null;
        this.baselineRow = -1;
        this.eof = true;
      } else {
        this.lineNo++;
        if (txt.length() < 64 + 1 + 1) {
          this.hash = null;
          this.baselineRow = -1;
          this.status = ExecutionStatus.failure("Could not read the baseline file '" + this.baseline
              + "'; Invalid format of line #" + this.lineNo + ": " + txt);
        } else {
          this.hash = txt.substring(0, 64);
          this.baselineRow = Long.parseLong(txt.substring(65));
        }
      }
    }
  }

  @Override
  public boolean consume(int liveRow, Hasher hasher) throws IOException, CloneNotSupportedException {
    if (this.status != null) {
      return false;
    }
    while (liveRow > this.baselineRow && !this.eof) {
      nextBaseline();
    }
    if (this.eof) {
//      log.info("CONSUME eof");
      error("Dump file end reached.");
      return false;
    }

    if (liveRow < this.baselineRow) {
//      log.info("CONSUME stepped over");
      return true;
    } else { // line == this.line
      String computed = hasher.getCurrentHash();
//      log.info(" -- computed " + computed);
      if (!computed.equals(this.hash)) {
//        log.info("CONSUME compare FAIL");
        this.status = ExecutionStatus.failure("Hash dump comparison failed: found different hashes in row #" + liveRow
            + " -- current hash: " + computed + " -- baseline hash: " + this.hash);
//        error("############################## Found different hash on line #" + line);
        return false;
      }
//      log.info("CONSUME compare SUCCESS");
      return true;
    }

  }

  @Override
  public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
//    log.info("CLOSE: this.eof=" + this.eof);
    String s = this.r.readLine();
//    log.info("CLOSE: s=" + s);

    this.r.close();
  }

  @Override
  public ExecutionStatus getStatus() {
    return this.status;
  }

  private void error(String txt) {
    System.out.println("ERROR " + txt);
  }

}