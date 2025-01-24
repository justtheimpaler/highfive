package highfive.commands.consumer;

import java.io.File;
import java.io.IOException;

import highfive.commands.HashDumpCommand.HashDumpConfig;
import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;

public interface HashDumpWriterFactory {

  public HashConsumer getInstance(HashDumpConfig config, File f)
      throws IOException, InvalidDumpFileException, DumpFileIOException;

  public static class HashDumpComparatorFactory implements HashDumpWriterFactory {

    private File baseline;

    public HashDumpComparatorFactory(File baseline) {
      this.baseline = baseline;
    }

    @Override
    public HashConsumer getInstance(HashDumpConfig config, File current)
        throws IOException, InvalidDumpFileException, DumpFileIOException {
      return new HashComparator(config.getTableName(), this.baseline, current);
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

}
