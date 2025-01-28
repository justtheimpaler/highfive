package highfive.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import highfive.exceptions.InvalidHashFileException;
import highfive.utils.Utl;

public class HashFile {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(HashFile.class.getName());

  private Map<String, TableHash> map = new LinkedHashMap<>();

  public void add(String hash, boolean nonDeterministic, boolean failed, long rowCount, String table)
      throws InvalidHashFileException {
    if (this.map.containsKey(table)) {
      throw new InvalidHashFileException("Duplicate table '" + table + "'.");
    }
    this.map.put(table, new TableHash(hash, nonDeterministic, failed, rowCount));
  }

  public void saveTo(final String file) throws IOException {
    try (Writer w = new BufferedWriter(new FileWriter(new File(file)))) {
      for (String table : this.map.keySet()) {
        TableHash h = this.map.get(table);
        w.write(h.getHash() + (h.isNonDeterministic() ? "*" : "") + (h.isFailed() ? "!" : "") + " " + h.getRowCount()
            + " " + table + "\n");
      }
    }
  }

  private static final Pattern LINE_PATTERN = Pattern.compile("^([0-9a-f]{64})(\\*?)(\\!?) ([0-9]+) (.+)$");

  public static HashFile loadFrom(final String file)
      throws FileNotFoundException, IOException, InvalidHashFileException {
    HashFile hf = new HashFile();
    try (BufferedReader r = new BufferedReader(new FileReader(new File(file)))) {
      String line;
      int lineNumber = 1;
      while ((line = r.readLine()) != null) {
        Matcher m = LINE_PATTERN.matcher(line);
        if (!m.matches()) {
          throw new InvalidHashFileException("Line #" + lineNumber //
              + " has an invalid hash format. Must be a 64-char hexa value, " //
              + "optionally followed by a star, " //
              + "optionally followed by an exclamation point, " //
              + "then a space and a table name (in lower case).");
        } else {
          String hash = m.group(1); // hash

          String star = m.group(2); // non-deterministic
          boolean nonDeterministic = star.equals("*");

          String exclamation = m.group(3); // failed
          boolean failed = exclamation.equals("!");

          String srows = m.group(4); // row count
          long rowCount = Long.parseLong(srows);

          String table = m.group(5); // table name

          hf.add(hash, nonDeterministic, failed, rowCount, table);
          lineNumber++;
        }
      }
    }
    return hf;
  }

  public static class ComparisonResult {

    private int matched = 0;
    private boolean nonDeterministic = false;
    private List<String> errors = new ArrayList<>();

    public void addMatched() {
      this.matched++;
    }

    public void addError(String error) {
      this.errors.add(error);
    }

    public int getMatched() {
      return matched;
    }

    public List<String> getErrors() {
      return errors;
    }

    public void setNonDeterministic() {
      this.nonDeterministic = true;
    }

    public boolean isNonDeterministic() {
      return nonDeterministic;
    }

  }

  private static final DecimalFormat DF = new DecimalFormat("#,##0");

  public ComparisonResult compareTo(final HashFile other, final String thisName, final String otherName) {

    ComparisonResult r = new ComparisonResult();

    for (String table : this.map.keySet()) {
      TableHash h = this.map.get(table);
      if (!other.map.containsKey(table)) {
        r.addError("Table '" + table + "' found in the " + thisName + ", but not in the " + otherName + ".");
      } else {
        TableHash o = other.map.get(table);

        if (h.isFailed() || o.isFailed()) {
          String where = h.isFailed() ? (o.isFailed() ? "the live and baseline tables" : "the baseline table")
              : "the live table";
          r.addError("Failed to compare hashes for the table '" + table
              + "' in the databases; failed to read and inspect " + where + ".");
        } else if (h.isNonDeterministic() || o.isNonDeterministic()) {
          String where = h.isNonDeterministic()
              ? (o.isNonDeterministic() ? "the live and baseline tables" : "the baseline table")
              : "the live table";
          r.addError("Failed to compare hashes for the table '" + table
              + "' in the databases; the hashing ordering is non-deterministic in " + where + ".");
        } else if (h.getRowCount() != o.getRowCount()) {
          r.addError("Failed to compare hashes for the table '" + table + "'; the current table has "
              + DF.format(h.getRowCount()) + " row(s) while the baseline table has " + DF.format(o.getRowCount())
              + " row(s).");
        } else if (Utl.distinct(h.getHash(), o.getHash())) {
          r.addError("Different hash values found for table '" + table + "' in the databases.");
        } else {
          r.addMatched();
        }
      }
    }

    for (String table : other.map.keySet()) {
      if (!this.map.containsKey(table)) {
        r.addError("Table '" + table + "' found in the " + otherName + ", but not in the " + thisName + ".");
      }
    }

    if (this.isNonDeterministic() || other.isNonDeterministic()) {
      r.setNonDeterministic();
    }

    return r;

  }

  public boolean isNonDeterministic() {
    for (TableHash h : this.map.values()) {
      if (h.isNonDeterministic()) {
        return true;
      }
    }
    return false;
  }

}
