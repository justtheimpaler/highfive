package highfive.commands.consumer;

import java.io.IOException;
import java.util.logging.Logger;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;
import highfive.utils.Utl;

public class HashLogger implements HashConsumer {

  private static final Logger log = Logger.getLogger(HashLogger.class.getName());

  private String tableName;
  private long start;
  private long end;

  private ExecutionStatus status;

  public HashLogger(String tableName, long start, long end) throws InvalidDumpFileException, DumpFileIOException {
    log.fine("init");
    this.tableName = tableName;
    this.start = start;
    this.end = end;
    this.status = null;
  }

  @Override
  public void consumeValueHeader(long row) {
    if (row >= this.start && row <= this.end) {
      log.info("    * Row #" + row + ":");
    }
  }

  @Override
  public void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException {
    if (row >= this.start && row <= this.end) {
      byte[] d = h.getInProgressDigest();
      log.info("      " + c.getName() + ": '" + c.getSerializer().getValue() + "' - encoded: " + Utl.toHex(bytes)
          + " -- hash: " + Utl.toHex(d));
    }
  }

  @Override
  public boolean consumeRow(long row, Hasher hasher)
      throws IOException, CloneNotSupportedException, InvalidDumpFileException, DumpFileIOException {
    return row < end;
  }

  @Override
  public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public ExecutionStatus getStatus() {
    return ExecutionStatus.success("");
  }

}