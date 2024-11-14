package test.model;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Processor {

  private static final Logger log = Logger.getLogger(Processor.class.getName());

  public void calc() {
    log.log(Level.SEVERE, "This is a SEVERE message");
    log.log(Level.WARNING, "This is a WARNING message");
    log.log(Level.INFO, "This is a INFO message: " + log.getName()+" level="+log.getLevel());
    log.log(Level.CONFIG, "This is a CONFIG message");
    log.log(Level.FINE, "This is a FINE message");
    log.log(Level.FINER, "This is a FINER message");
    log.log(Level.FINEST, "This is a FINEST message");
  }

}
