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

  private Map<String, String> map = new LinkedHashMap<>();

  public void add(String hash, String table) throws InvalidHashFileException {
    if (this.map.containsKey(table)) {
      throw new InvalidHashFileException("Duplicate table t.");
    }
    this.map.put(table, hash);
  }

  public void saveTo(final String file) throws IOException {
    try (Writer w = new BufferedWriter(new FileWriter(new File(file)))) {
      for (String table : this.map.keySet()) {
        w.write(this.map.get(table) + " " + table + "\n");
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
        if (!line.matches("^[0-9a-f]{64} .+$")) {
          throw new InvalidHashFileException("Line #" + lineNumber
              + " has an invalid hash format. Must be a 40-char hexa value, a space, and a table name (in lower case).");
        }
        String hash = line.substring(0, 64);
        String table = line.substring(65);
        hf.add(hash, table);
        lineNumber++;
      }
    }
    return hf;
  }

  public List<String> compareTo(final HashFile other, final String thisName, final String otherName) {

    List<String> msg = new ArrayList<>();

    for (String table : this.map.keySet()) {
      String hash = this.map.get(table);
      if (!other.map.containsKey(table)) {
        msg.add("Table '" + table + "' found in the " + thisName + ", but not in the " + otherName + ".");
      } else {
        String ohash = other.map.get(table);
        if (Utl.distinct(hash, ohash)) {
          msg.add("Different hash values found for table '" + table + "' in the databases.");
        }
      }
    }

    for (String table : other.map.keySet()) {
      if (!this.map.containsKey(table)) {
        msg.add("Table '" + table + "' found in the " + otherName + ", but not in the " + thisName + ".");
      }
    }

    return msg;

  }

}
