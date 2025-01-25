package highfive.commands.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;
import highfive.utils.Utl;

public class RangeHashDumpWriter implements HashConsumer {

  private BufferedWriter w;
  private long start;
  private long end;

  public RangeHashDumpWriter(String tableName, File f, long start, long end) throws IOException {
    this.w = new BufferedWriter(new FileWriter(f));
    this.w.write("# table: " + tableName + " (rows " + start + "-" + end + ")\n");
    this.start = start;
    this.end = end;
  }

  @Override
  public void consumeValueHeader(long row) {
  }

  @Override
  public void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException {
  }

  @Override
  public boolean consumeRow(long row, Hasher hasher) throws IOException, CloneNotSupportedException {
    if (row >= this.start && row <= this.end) {
      this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + row + "\n");
    }
    return row <= this.end;
  }

  @Override
  public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    this.w.close();
  }

  @Override
  public ExecutionStatus getStatus() {
    return ExecutionStatus.success("Ranged hash file generated.");
  }

}