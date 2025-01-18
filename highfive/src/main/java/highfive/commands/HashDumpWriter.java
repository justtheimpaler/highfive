package highfive.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.model.Hasher;
import highfive.utils.Utl;

public interface HashDumpWriter extends AutoCloseable {

  boolean write(int line, Hasher hasher) throws IOException, CloneNotSupportedException;

  public static interface HashDumpWriterFactory {
    public HashDumpWriter getInstance(HashDumpConfig config, File f) throws IOException;
  }

  public static class FullHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashDumpWriter getInstance(HashDumpConfig config, File f) throws IOException {
      return new FullHashDumpWriter(config.getTableName(), f);
    }

  }

  public static class RangeHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashDumpWriter getInstance(HashDumpConfig config, File f) throws IOException {
      return new RangeHashDumpWriter(config.getTableName(), f, config.getStart(), config.getEnd());
    }

  }

  public static class SteppedHashDumpWriterFactory implements HashDumpWriterFactory {

    @Override
    public HashDumpWriter getInstance(HashDumpConfig config, File f) throws IOException {
      return new SteppedRangeHashDumpWriter(config.getTableName(), f, config.getStart(), config.getEnd(),
          config.getStep());
    }

  }

  public static class NullHashDumpWriter implements HashDumpWriter {

    @Override
    public boolean write(int line, Hasher hasher) throws IOException {
      return true;
    }

    @Override
    public void close() throws Exception {
    }

  }

  public static class FullHashDumpWriter implements HashDumpWriter {

    private BufferedWriter w;

    public FullHashDumpWriter(String tableName, File f) throws IOException {
      super();
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " -- full table\n");
    }

    @Override
    public boolean write(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
      this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
      return true;
    }

    @Override
    public void close() throws Exception {
      this.w.close();
    }

  }

  public static class RangeHashDumpWriter implements HashDumpWriter {

    private BufferedWriter w;
    private long start;
    private long end;

    public RangeHashDumpWriter(String tableName, File f, long start, long end) throws IOException {
      super();
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " -- start: " + start + ", end: " + end + "\n");
      this.start = start;
      this.end = end;
    }

    @Override
    public boolean write(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
      if (line >= this.start && line <= this.end) {
        this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
      }
      return line <= this.end;
    }

    @Override
    public void close() throws Exception {
      this.w.close();
    }

  }

  public static class SteppedRangeHashDumpWriter implements HashDumpWriter {

    private BufferedWriter w;
    private long start;
    private long end;
    private long step;
    private long nextLine;

    public SteppedRangeHashDumpWriter(String tableName, File f, long start, long end, long step) throws IOException {
      super();
      this.w = new BufferedWriter(new FileWriter(f));
      this.w.write("# table: " + tableName + " -- start: " + start + ", end: " + end + ", step: " + step + "\n");
      this.start = start;
      this.end = end;
      this.step = step;
      this.nextLine = start;
    }

    @Override
    public boolean write(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
      if (line >= this.start && line <= this.end) {
        if (line == this.nextLine) {
          this.w.write(Utl.toHex(hasher.getInProgressDigest()) + " " + line + "\n");
          this.nextLine = this.nextLine + this.step;
        }
      }
      return line <= this.end;
    }

    @Override
    public void close() throws Exception {
      this.w.close();
    }

  }

}
