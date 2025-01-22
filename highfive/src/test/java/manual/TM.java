package manual;

import java.io.File;
import java.util.logging.Level;

import highfive.commands.consumer.DumpFileReader;
import highfive.commands.consumer.DumpFileReader.DumpFileIOException;
import highfive.commands.consumer.DumpFileReader.InvalidDumpFileException;
import highfive.utils.JULCustomFormatter;

public class TM {

  static {
    JULCustomFormatter.initialize(Level.INFO);
  }

  public static void main(String[] args) throws InvalidDumpFileException, DumpFileIOException {
    DumpFileReader r = new DumpFileReader(new File("ss.dump"));
    System.out.println("File: " + r.getMetadata());
    while (r.next()) {
      System.out.println(" - row #" + r.getRow() + " - " + r.getHash());
    }
    System.out.println("-- end of file");
  }

}
