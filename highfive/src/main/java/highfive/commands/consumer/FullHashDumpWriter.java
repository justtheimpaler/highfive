package highfive.commands.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
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
  public boolean consume(long line, Hasher hasher) throws IOException, CloneNotSupportedException {
    this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
    return true;
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
    return ExecutionStatus.success("Full hash dump generated");
  }

}