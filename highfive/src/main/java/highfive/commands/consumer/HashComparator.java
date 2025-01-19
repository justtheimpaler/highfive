package highfive.commands.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

import highfive.commands.HashConsumer;
import highfive.exceptions.InvalidHashFileException;
import highfive.model.Hasher;

public class HashComparator implements HashConsumer {

  private static final Logger log = Logger.getLogger(HashComparator.class.getName());

  private BufferedReader r;

  private boolean eof;
  private String hash;
  private long line;

  public HashComparator(String tableName, File baseline) throws IOException {
    this.eof = false;
    this.r = new BufferedReader(new FileReader(baseline));
    readCommentLine(); // skip the first line since it's a comment
    readLine();
  }

  private void readCommentLine() throws IOException {
    log.info("readCommentLine() eof=" + this.eof);
    if (!this.eof) {
      this.r.readLine();
    }
  }

  private void readLine() throws IOException {
    log.info("readLine() eof=" + this.eof);
    if (!this.eof) {
      String txt = this.r.readLine();
      log.info(" -- read " + txt);
      if (txt == null || txt.length() < 64 + 1 + 1) {
        log.info(" --> fail");
        this.eof = true;
      } else {
        this.hash = txt.substring(0, 64);
//        String sep = txt.substring(64, 65);
        this.line = Long.parseLong(txt.substring(65));
        log.info(" --> line #" + this.line + " hash=" + this.hash);
      }
    }
  }

  @Override
  public boolean consume(int line, Hasher hasher) throws IOException, CloneNotSupportedException {
    log.info("CONSUME #" + line);
    while (line > this.line && !this.eof) {
      log.info("CONSUME advance");
      readLine();
    }
    if (this.eof) {
      log.info("CONSUME eof");
      error("Dump file end reached.");
      return false;
    }

    if (line < this.line) {
      log.info("CONSUME stepped over");
      return true;
    } else { // line == this.line
      if (!hasher.same(this.hash)) {
        log.info("CONSUME compare FAIL");
        error("Found different hash on line #" + line);
        return false;
      }
      log.info("CONSUME compare SUCCESS");
      return true;
    }

  }

  @Override
  public void closeEntry(String genericName, boolean hasOrderingErrors) throws InvalidHashFileException {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    log.info("CLOSE: this.eof=" + this.eof);
    String s = this.r.readLine();
    log.info("CLOSE: s=" + s);

    this.r.close();
  }

  private void error(String txt) {
    System.out.println("ERROR " + txt);
  }

}