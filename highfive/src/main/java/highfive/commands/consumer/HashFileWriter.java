package highfive.commands.consumer;

import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.HashFile;
import highfive.model.Hasher;
import highfive.utils.Utl;

public class HashFileWriter implements HashConsumer {

  private String filename;
  private HashFile hashFile;
  private Hasher lastHasher;

  public HashFileWriter(String filename) {
    this.filename = filename;
    this.hashFile = new HashFile();
    this.lastHasher = null;
  }

  @Override
  public void initializeHasher(Hasher h) {
    this.lastHasher = h;
  }

  @Override
  public void consumeValueHeader(long row) {
  }

  @Override
  public void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException {
  }

  @Override
  public boolean consumeRow(long row, Hasher hasher) throws IOException {
    this.lastHasher = hasher;
    return true;
  }

  public HashFile getHashFile() {
    return hashFile;
  }

  @Override
  public void consumeTable(String genericName, boolean nonDeterministic, boolean failed, long rowCount)
      throws InvalidHashFileException {
    String hash = Utl.toHex(this.lastHasher.close());
    hashFile.add(hash, nonDeterministic, failed, rowCount, genericName);
  }

  @Override
  public void close() throws Exception {
//    System.out.println(">> HashFileWriter.close()");
    this.hashFile.saveTo(this.filename);
  }

  @Override
  public ExecutionStatus getStatus() {
    return ExecutionStatus.success("Hash file generated.");
  }

}