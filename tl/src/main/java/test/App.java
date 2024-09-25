package test;

import java.util.logging.Level;
import java.util.logging.Logger;

import test.jul.JULCustomFormatter;
import test.model.Processor;

public class App {

  private static final Logger log = Logger.getLogger(App.class.getName());

  static {
    JULCustomFormatter.initialize(Level.INFO);
  }

  public static void main(final String[] args) {
    log.info("Starting processing");
    App j = new App();
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
