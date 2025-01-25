package highfive.commands.consumer;

import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;

public class NullHashDumpWriter implements HashConsumer {

  @Override
  public void consumeValueHeader(long row) {
  }

  @Override
  public void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException {
  }

  @Override
  public boolean consumeRow(long row, Hasher hasher) throws IOException {
    return true;
  }

  @Override
  public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public ExecutionStatus getStatus() {
    return ExecutionStatus.success("");
  }

}