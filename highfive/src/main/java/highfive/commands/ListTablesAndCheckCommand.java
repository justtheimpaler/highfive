package highfive.commands;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import highfive.exceptions.InvalidConfigurationException;
import highfive.exceptions.InvalidSchemaException;
import highfive.exceptions.UnsupportedDatabaseTypeException;
import highfive.model.Column;
import highfive.model.Identifier;
import highfive.model.Table;

public class ListTablesAndCheckCommand extends DataSourceCommand {

  public ListTablesAndCheckCommand(final String datasourceName)
      throws InvalidConfigurationException, SQLException, UnsupportedDatabaseTypeException {
    super("List Tables", datasourceName);
  }

  @Override
  public void execute()
      throws SQLException, UnsupportedDatabaseTypeException, InvalidSchemaException, InvalidConfigurationException {

    // 1. List Tables

    List<Identifier> tableNames = this.ds.getDialect().listTablesNames();

    int filterSize = this.ds.getTableFilter().size();
    info(" ");
    info("Tables found (" + tableNames.size() + (filterSize == 0 ? "" : ("/" + filterSize)) + "):");
    for (Identifier t : tableNames) {
      info("  " + t.getGenericName());
    }
    Set<String> na = this.ds.getTableFilter().listNotAccepted();
    if (!na.isEmpty()) {
      info("Tables not found (" + na.size() + "/" + filterSize + "):");
      for (String n : na) {
        info("  " + n);
      }
    }

    // 2. Summary

    Map<String, Integer> typeCount = new TreeMap<>();

    List<Table> tables = new ArrayList<>();
    for (Identifier tn : tableNames) {
      Table t = this.ds.getDialect().getTableMetaData(tn);
      tables.add(t);
      for (Column c : t.getColumns()) {
        String key = c.getRenderedType() + " [" + (c.getSerializer() == null ? "N/A" : c.getSerializer().getName())
            + "]";
        Integer count = typeCount.get(key);
        count = count == null ? Integer.valueOf(1) : count + 1;
        typeCount.put(key, count);
      }
    }

    info(" ");
    info("Summary of types found (" + typeCount.size() + "):");
    for (String t : typeCount.keySet()) {
      info("  " + t + ": " + typeCount.get(t));
    }

    // 3. Display Row Count

    displayRowCount(tables);

    // 4. Check Hashing Preconditions

    checkIfHashingAndCopyingIsSupported(tables);

  }

}
