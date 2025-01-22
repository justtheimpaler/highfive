package highfive.commands.consumer;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Hasher;

public class HashComparator implements HashConsumer {

  private static final Logger log = Logger.getLogger(HashComparator.class.getName());

  private File baseline;
  private DumpFileReader r;
  private boolean eof;

  private ExecutionStatus status;

  public HashComparator(String tableName, File baseline, File current)
      throws InvalidDumpFileException, DumpFileIOException {
    log.fine("init");
    this.baseline = baseline;
    this.r = new DumpFileReader(baseline);
    this.eof = !this.r.next();
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