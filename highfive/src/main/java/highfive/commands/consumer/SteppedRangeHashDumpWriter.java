package highfive.commands.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;
import highfive.utils.Utl;

public class SteppedRangeHashDumpWriter implements HashConsumer {

  private BufferedWriter w;
  private long start;
  private long end;
  private long step;
  private long nextLine;

  public SteppedRangeHashDumpWriter(String tableName, File f, long start, long end, long step) throws IOException {
    this.w = new BufferedWriter(new FileWriter(f));
    this.w.write("# table: " + tableName + " (rows " + start + "-" + end + ", step " + step + ")\n");
    this.start = start;
    this.end = end;
    this.step = step;
    this.nextLine = start;
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
      if (row == this.nextLine) {
        this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + row + "\n");
        this.nextLine = this.nextLine + this.step;
      }
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