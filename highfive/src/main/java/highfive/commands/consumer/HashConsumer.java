package highfive.commands.consumer;

import java.io.IOException;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Hasher;

public interface HashConsumer extends AutoCloseable {

  boolean consume(int line, Hasher hasher)
      throws IOException, CloneNotSupportedException, InvalidDumpFileException, DumpFileIOException;

  void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException;

  public static class ExecutionStatus {

    private boolean successful;
    private String errorMessage;

    private ExecutionStatus(boolean successful, String errorMessage) {
      this.successful = successful;
      this.errorMessage = errorMessage;
    }

    public static ExecutionStatus success() {
      return new ExecutionStatus(true, null);
    }

    public static ExecutionStatus failure(String errorMessage) {
      return new ExecutionStatus(false, errorMessage);
    }

    public boolean successful() {
      return successful;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public String toString() {
      return "ExecutionStatus [successful=" + successful + ", errorMessage=" + errorMessage + "]";
    }

  }

  ExecutionStatus getStatus();

}
