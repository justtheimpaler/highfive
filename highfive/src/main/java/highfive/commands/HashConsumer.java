package highfive.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.commands.consumer.HashComparator;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.HashFile;
import highfive.model.Hasher;
import highfive.utils.Utl;

public interface HashConsumer extends AutoCloseable {

  boolean consume(int line, Hasher hasher) throws IOException, CloneNotSupportedException;

  void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException;

  public static interface HashDumpWriterFactory {
    public HashConsumer getInstance(HashDumpConfig config, File f) throws IOException;
  }

  public static class HashDumpComparatorFactory implements HashDumpWriterFactory {

    @Override
    public HashConsumer getInstance(HashDumpConfig config, File baseline) throws IOException {
      return new HashComparator(config.getTableName(), baseline);
    }

  }

  public static class FullHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashConsumer getInstance(HashDumpConfig config, File f) throws IOException {
      return new FullHashDumpWriter(config.getTableName(), f);
    }

  }

  public static class RangeHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashConsumer getInstance(HashDumpConfig config, File f) throws IOException {
      return new RangeHashDumpWriter(config.getTableName(), f, config.getStart(), config.getEnd());
    }

  }

  public static class SteppedHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashConsumer getInstance(HashDumpConfig config, File f) throws IOException {
      return new SteppedRangeHashDumpWriter(config.getTableName(), f, config.getStart(), config.getEnd(),
          config.getStep());
    }

  }

  public static class HashFileWriter implements HashConsumer {

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
//      System.out.println(">> HashFileWriter.closeEntry() - hash=" + hash);
      hashFile.add(hash, genericName, nonDeterministic);
    }

    @Override
    public void close() throws Exception {
//      System.out.println(">> HashFileWriter.close()");
      this.hashFile.saveTo(this.filename);
    }

  }

  public static class NullHashDumpWriter implements HashConsumer {

    @Override
    public boolean consume(int line, Hasher hasher) throws IOException {
      return true;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
      // Nothing to do
    }

  }

  public static class FullHashDumpWriter implements HashConsumer {

    private BufferedWriter w;

    public FullHashDumpWriter(String tableName, File f) throws IOException {
      super();
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " (whole table)\n");
    }

    @Override
    public boolean consume(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
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

  }

  public static class RangeHashDumpWriter implements HashConsumer {

    private BufferedWriter w;
    private long start;
    private long end;

    public RangeHashDumpWriter(String tableName, File f, long start, long end) throws IOException {
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " (range " + start + "-" + end + ")\n");
      this.start = start;
      this.end = end;
    }

    @Override
    public boolean consume(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
      if (line >= this.start && line <= this.end) {
        this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
      }
      return line <= this.end;
    }

    @Override
    public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
      // Nothing to do
    }

    @Override
    public void close() throws Exception {
      this.w.close();
    }

  }

  public static class SteppedRangeHashDumpWriter implements HashConsumer {

    private BufferedWriter w;
    private long start;
    private long end;
    private long step;
    private long nextLine;

    public SteppedRangeHashDumpWriter(String tableName, File f, long start, long end, long step) throws IOException {
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " (range " + start + "-" + end + ", step " + step + ")\n");
      this.start = start;
      this.end = end;
      this.step = step;
      this.nextLine = start;
    }

    @Override
    public boolean consume(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
      if (line >= this.start && line <= this.end) {
        if (line == this.nextLine) {
          this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
          this.nextLine = this.nextLine + this.step;
        }
      }
      return line <= this.end;
    }

    @Override
    public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
      // Nothing to do
    }

    @Override
    public void close() throws Exception {
      this.w.close();
    }

  }

}
