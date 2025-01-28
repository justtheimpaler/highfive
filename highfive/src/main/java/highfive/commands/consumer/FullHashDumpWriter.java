package highfive.commands.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;
import highfive.utils.Utl;

public class FullHashDumpWriter implements HashConsumer {

  private BufferedWriter w;

  public FullHashDumpWriter(String tableName, File f) throws IOException {
    super();
    this.w = new BufferedWriter(new FileWriter(f));
    this.w.write("# table: " + tableName + " (whole table)\n");
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
  public boolean consumeRow(long row, Hasher hasher) throws IOException, CloneNotSupportedException {
    this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + row + "\n");
    return true;
  }

  @Override
  public void consumeTable(String genericName, boolean nonDeterministic, boolean failed, long rowCount)
      throws InvalidHashFileException {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    this.w.close();
  }

  @Override
  public ExecutionStatus getStatus() {
    return ExecutionStatus.success("Full hash dump generated");
  }

}