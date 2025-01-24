package highfive.commands.consumer;

import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
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
  }

  @Override
  public boolean consume(int line, Hasher hasher) throws IOException {
    this.lastHasher = hasher;
    return true;
  }

  public HashFile getHashFile() {
    return hashFile;
  }

  @Override
  public void closeEntry(String genericName, boolean nonDeterministic) throws InvalidHashFileException {
    String hash = Utl.toHex(this.lastHasher.close());
//    System.out.println(">> HashFileWriter.closeEntry() - hash=" + hash);
    hashFile.add(hash, genericName, nonDeterministic);
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