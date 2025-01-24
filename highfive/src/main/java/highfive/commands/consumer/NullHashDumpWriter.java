package highfive.commands.consumer;

import java.io.IOException;

import highfive.exceptions.InvalidHashFileException;
import highfive.model.Hasher;

public class NullHashDumpWriter implements HashConsumer {

  @Override
  public boolean consume(int line, Hasher hasher) throws IOException {
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