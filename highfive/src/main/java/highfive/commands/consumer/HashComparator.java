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
    this.eof = false;
  }

  @Override
  public boolean consume(int liveRow, Hasher hasher)
      throws IOException, CloneNotSupportedException, InvalidDumpFileException, DumpFileIOException {
    if (this.status != null) {
      return false;
    }
    while (!this.eof && (r.atStart() || r.getRow() < liveRow)) {
      if (!r.next()) {
        this.eof = true;
      }
    }
    if (this.eof) {
      this.status = ExecutionStatus.failure(
          "The end of the baseline file was reached and could not find the hash for the database row #" + liveRow);
      return false;
    }

    if (r.getRow() > liveRow) {
//      log.info("CONSUME stepped over");
      return true;
    } else { // line == this.line
      String liveHash = hasher.getOngoingHash();
//      log.info(" -- computed " + computed);
      if (!liveHash.equals(r.getHash())) {
//        log.info("CONSUME compare FAIL");
        this.status = ExecutionStatus.failure("Hash dump comparison failed: found different hashes in row #" + liveRow
            + " -- current hash: " + liveHash + " -- baseline hash: " + r.getHash());
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
//    String s = this.r.readLine();
//    log.info("CLOSE: s=" + s);

    this.r.close();
  }

  @Override
  public ExecutionStatus getStatus() {
    return this.status;
  }

}