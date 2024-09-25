package tl;

import java.util.logging.Level;
import java.util.logging.Logger;

import tl.jul.JULCustomFormatter;
import tl.model.Processor;

public class TestJUL {

  private static final Logger log = Logger.getLogger(TestJUL.class.getName());

  static {
    JULCustomFormatter.initialize(Level.FINEST);
  }

  public static void main(final String[] args) {
    log.info("Starting processing");
    TestJUL j = new TestJUL();
    j.compute1();

    new Processor().calc();
    log.info("-- Processing complete -- Exiting now.");
  }

  private void compute1() {
    log.info("Starting process...");
    // do something
    log.info("Ending process...");
  }

}
