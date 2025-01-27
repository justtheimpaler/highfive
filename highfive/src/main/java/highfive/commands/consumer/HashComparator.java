package highfive.commands.consumer;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.DumpFileType;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;

public class HashComparator implements HashConsumer {

  private static final Logger log = Logger.getLogger(HashComparator.class.getName());

  private String tableName;
  @SuppressWarnings("unused")
  private File baseline;
  private DumpFileReader b;
  private boolean beof;

  private Long lastMatchedRow;
  private ExecutionStatus status;

  public HashComparator(String tableName, File baseline, File current)
      throws InvalidDumpFileException, DumpFileIOException {
    log.fine("init");
    this.tableName = tableName;
    this.baseline = baseline;
    this.b = new DumpFileReader(baseline);
    this.beof = false;
    this.lastMatchedRow = null;
    this.status = null;
  }

  @Override
  public void initializeHasher(Hasher h) {
    // Nothing to do
  }

  @Override
  public void consumeValueHeader(long row) {
  }

  @Override
  public void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException {
  }

  @Override
  public boolean consumeRow(long liveRow, Hasher hasher)
      throws IOException, CloneNotSupportedException, InvalidDumpFileException, DumpFileIOException {
    if (this.status != null) {
      return false;
    }
    while (!this.beof && (b.atStart() || b.getRow() < liveRow)) {
      nextBaseline();
    }
    if (this.beof) {
      if (b.getMetadata().getType() == DumpFileType.FULL) {
        this.status = ExecutionStatus.failure("Found more rows in the live table '" + this.tableName
            + "' than in the baseline file; no matching baseline hash for live row #" + liveRow);
        return false;
      } else {
        this.status = ExecutionStatus
            .success("The live table '" + this.tableName + "' fully matches the partial baseline dump file" + " (rows "
                + b.getMetadata().getStart() + "-" + b.getMetadata().getEnd() + ").");
        return false;
      }
    }

    if (b.getRow() > liveRow) {
      return true;
    } else { // line == this.line
      String liveHash = hasher.getOngoingHash();
      if (!liveHash.equals(b.getHash())) {
        computeErrorStatus(liveRow, liveHash);
        nextBaseline();
        return false;
      }
      this.lastMatchedRow = liveRow;
      nextBaseline();
      return true;
    }
  }

  private void computeErrorStatus(long liveRow, String liveHash) {
    switch (b.getMetadata().getType()) {
    case FULL:
      this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '" + this.tableName
          + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash());
      break;
    case RANGED:
      if (this.lastMatchedRow == null && liveRow > 1) {
        this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '"
            + this.tableName + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash()
            + "\n * Note: Since this is a ranged baseline hash dump file, the different row could be in row #" + liveRow
            + ", or a row before it.");
      } else {
        this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '"
            + this.tableName + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash());
      }
      break;
    case STEPPED:
      if (this.lastMatchedRow == null) {
        if (liveRow > 1) {
          this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '"
              + this.tableName + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash()
              + "\n * Note: Since this is a stepped baseline hash dump file, the different row could be in row #"
              + liveRow + ", or a row before it.");
        } else {
          this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '"
              + this.tableName + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash());
        }
      } else {
        this.status = ExecutionStatus.failure("Found different hashes in row #" + liveRow + " in table '"
            + this.tableName + "' -- current hash: " + liveHash + " -- baseline hash: " + b.getHash()
            + "\n * Note: Since this is a stepped baseline hash dump file, the different row must be between rows #"
            + (this.lastMatchedRow + 1) + " and #" + liveRow + ".");
      }
      break;
    }
  }

  private void nextBaseline() throws InvalidDumpFileException, DumpFileIOException {
    if (!b.next()) {
      this.beof = true;
    }
  }

  @Override
  public void consumeTable(String genericName, boolean hasOrderingErrors, long rowCount)
      throws InvalidHashFileException {
    if (this.status != null) {
      return;
    }
    if (!this.beof) {
      this.status = ExecutionStatus.failure("Found more rows in the baseline file than in the live table '"
          + this.tableName + "'; the table does not a row #" + b.getRow());
    } else {
      if (b.getMetadata().getType() == DumpFileType.FULL) {
        this.status = ExecutionStatus
            .success("The live table '" + this.tableName + "' fully matches the baseline dump file.");
      } else {
        this.status = ExecutionStatus
            .success("The live table '" + this.tableName + "' fully matches the partial baseline dump file (rows "
                + b.getMetadata().getStart() + "-" + b.getMetadata().getEnd() + ").");
      }
    }
  }

  @Override
  public void close() throws Exception {
    this.b.close();
  }

  @Override
  public ExecutionStatus getStatus() {
    return this.status;
  }

}