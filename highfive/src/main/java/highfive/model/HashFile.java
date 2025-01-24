package highfive.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import highfive.exceptions.InvalidHashFileException;
import highfive.utils.Utl;

public class HashFile {

  private Map<String, Hash> map = new LinkedHashMap<>();

  private static class Hash {

    private String hash;
    private boolean nonDeterministic;

    public Hash(String hash, boolean nonDeterministic) {
      super();
      this.hash = hash;
      this.nonDeterministic = nonDeterministic;
    }

    public String getHash() {
      return hash;
    }

    public boolean isNonDeterministic() {
      return nonDeterministic;
    }

  }

  public void add(String hash, String table, boolean nonDeterministic) throws InvalidHashFileException {
    if (this.map.containsKey(table)) {
      throw new InvalidHashFileException("Duplicate table '" + table + "'.");
    }
    this.map.put(table, new Hash(hash, nonDeterministic));
  }

  public void saveTo(final String file) throws IOException {
    try (Writer w = new BufferedWriter(new FileWriter(new File(file)))) {
      for (String table : this.map.keySet()) {
        Hash h = this.map.get(table);
        w.write(h.getHash() + (h.isNonDeterministic() ? "*" : "") + " " + table + "\n");
      }
    }
  }

  public static HashFile loadFrom(final String file)
      throws FileNotFoundException, IOException, InvalidHashFileException {
    HashFile hf = new HashFile();
    try (BufferedReader r = new BufferedReader(new FileReader(new File(file)))) {
      String line;
      int lineNumber = 1;
      while ((line = r.readLine()) != null) {
        if (!line.matches("^[0-9a-f]{64}\\*? .+$")) {
          throw new InvalidHashFileException("Line #" + lineNumber
              + " has an invalid hash format. Must be a 64-char hexa value (optionally followed by a star), "
              + "a space, and a table name (in lower case).");
        }
        String hash = line.substring(0, 64);
        boolean nonDeterministic;
        String table;
        if (line.charAt(64) == '*') {
          nonDeterministic = true;
          table = line.substring(66);
        } else {
          nonDeterministic = false;
          table = line.substring(65);
        }
        hf.add(hash, table, nonDeterministic);
        lineNumber++;
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

  public ComparisonResult compareTo(final HashFile other, final String thisName, final String otherName) {

    ComparisonResult r = new ComparisonResult();

    for (String table : this.map.keySet()) {
      Hash h = this.map.get(table);
      if (!other.map.containsKey(table)) {
        r.addError("Table '" + table + "' found in the " + thisName + ", but not in the " + otherName + ".");
      } else {
        Hash o = other.map.get(table);
        if (Utl.distinct(h.getHash(), o.getHash())) {
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
    for (Hash h : this.map.values()) {
      if (h.isNonDeterministic()) {
        return true;
      }
    }
    return false;
  }

}
