package highfive.commands.consumer;

import java.io.IOException;

import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Column;
import highfive.model.Hasher;

public interface HashConsumer extends AutoCloseable {

  void consumeValueHeader(long row);

  void consumeValue(long row, Column c, byte[] bytes, Hasher h) throws CloneNotSupportedException;

  boolean consumeRow(long row, Hasher hasher)
      throws IOException, CloneNotSupportedException, InvalidDumpFileException, DumpFileIOException;

  void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException;

  public static class ExecutionStatus {

    private boolean successful;
    private String message;

    private ExecutionStatus(boolean successful, String message) {
      this.successful = successful;
      this.message = message;
    }

    public static ExecutionStatus success(String message) {
      return new ExecutionStatus(true, message);
    }

    public static ExecutionStatus failure(String message) {
      return new ExecutionStatus(false, message);
    }

    public boolean successful() {
      return successful;
    }

    public String getMessage() {
      return message;
    }

    @Override
    public String toString() {
      return "ExecutionStatus [successful=" + successful + ", message=" + message + "]";
    }

  }

  ExecutionStatus getStatus();

}
